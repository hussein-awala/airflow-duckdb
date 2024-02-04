from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import kubernetes.client as k8s
import sqlparse
from airflow.providers.cncf.kubernetes import __version__
from airflow.providers.cncf.kubernetes.kubernetes_helper_functions import add_pod_suffix
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from packaging import version

if TYPE_CHECKING:
    from typing import Any

    import jinja2
    from airflow.utils.context import Context


@dataclass(kw_only=True)
class S3FSConfig:
    """S3 Filesystem configuration."""

    endpoint: str | None = None
    access_key_id: str | None = None
    secret_access_key: str | None = None
    region: str | None = None
    use_ssl: bool = True


class DuckDBPodOperator(KubernetesPodOperator):
    """Runs a task using a KubernetesPodOperator with the DuckDB image."""

    template_fields = (*KubernetesPodOperator.template_fields, "query", "s3_fs_config")
    template_ext = (".sql", ".sql.j2")

    def __init__(
        self,
        query: str,
        s3_fs_config: S3FSConfig | None = None,
        image: str = "ghcr.io/hussein-awala/airflow-duckdb:latest",
        name: str = "duckdb",
        **kwargs,
    ):
        self.query = query
        self.s3_fs_config = s3_fs_config
        super().__init__(
            image=image,
            name=name,
            **kwargs,
        )

    def _render_nested_template_fields(
        self,
        content: Any,
        context: Context,
        jinja_env: jinja2.Environment,
        seen_oids: set,
    ) -> None:
        """Render nested template fields."""
        if id(content) not in seen_oids:
            template_fields: tuple | None

            if isinstance(content, S3FSConfig):
                template_fields = ("endpoint", "access_key_id", "secret_access_key", "region")
            else:
                template_fields = None

            if template_fields:
                seen_oids.add(id(content))
                self._do_render_template_fields(content, template_fields, context, jinja_env, seen_oids)
                return

        super()._render_nested_template_fields(content, context, jinja_env, seen_oids)

    def _create_k8s_secret(self, *, namespace: str, secret_data: dict[str, str]) -> str:
        """Create a Kubernetes secret."""

        secret = k8s.V1Secret(
            metadata=k8s.V1ObjectMeta(
                name=add_pod_suffix(pod_name="duckdb-secret"),
                labels=self.labels,
                annotations=self.annotations,
            ),
            type="Opaque",
            string_data=secret_data,
        )
        self.client.create_namespaced_secret(namespace, secret)
        return secret.metadata.name

    def execute(self, context):
        """Execute the operator."""
        query = sqlparse.format(self.query, strip_comments=True)
        # split the query by semicolon
        parsed_query = sqlparse.split(query)
        # create a list of pre-execution commands
        secret_name: str | None = None
        pre_execution = []
        secret_namespace = (
            self.namespace or self.hook.get_namespace() or self._incluster_namespace or "default"
        )
        if self.do_xcom_push:
            # if xcom_push is enabled, we will save the last SELECT statement as a JSON file
            last_query = parsed_query[-1]
            if last_query.strip().upper().startswith("SELECT"):
                last_query = (
                    f"COPY ({last_query.strip(';')})"
                    "TO '/airflow/xcom/return.json' (FORMAT JSON, ARRAY true);"
                )
            parsed_query = parsed_query[:-1] + [last_query]
        if self.s3_fs_config:
            # if S3 filesystem is configured, we will load the S3 filesystem and set the configurations
            pre_execution.extend(
                [
                    "LOAD httpfs;",
                    "LOAD aws;",
                    "CALL load_aws_credentials();",
                    "SET s3_url_style='path';",
                ]
            )
            # set the S3 filesystem configurations from the S3FSConfig
            # (useful for local development with MinIO)
            if self.s3_fs_config.endpoint:
                pre_execution.append(f"SET s3_endpoint = '{self.s3_fs_config.endpoint}';")
            pre_execution.append(f"SET s3_use_ssl = {'true' if self.s3_fs_config.use_ssl else 'false'};")
            secret_data = {}
            if self.s3_fs_config.access_key_id:
                secret_data["AWS_ACCESS_KEY_ID"] = self.s3_fs_config.access_key_id
            if self.s3_fs_config.secret_access_key:
                secret_data["AWS_SECRET_ACCESS_KEY"] = self.s3_fs_config.secret_access_key
            if self.s3_fs_config.region:
                secret_data["AWS_REGION"] = self.s3_fs_config.region
            if secret_data:
                secret_name = self._create_k8s_secret(
                    namespace=secret_namespace,
                    secret_data={
                        "AWS_ACCESS_KEY_ID": self.s3_fs_config.access_key_id,
                        "AWS_SECRET_ACCESS_KEY": self.s3_fs_config.secret_access_key,
                    },
                )
        # reconstruct the query
        self.query = "\n".join(pre_execution + parsed_query)
        self.arguments = [
            "-s",
            self.query,
        ]
        if secret_name:
            self.secrets += [
                Secret("env", None, secret=secret_name),
            ]
            if version.parse(__version__) >= version.parse("7.14.0"):
                from airflow.providers.cncf.kubernetes.callbacks import KubernetesPodOperatorCallback

                class DuckDBPodOperatorCallback(KubernetesPodOperatorCallback):
                    @staticmethod
                    def on_pod_creation(
                        *, pod: k8s.V1Pod, client: k8s.CoreV1Api, mode: str, **kwargs
                    ) -> None:
                        secret = client.read_namespaced_secret(
                            name=secret_name,
                            namespace=secret_namespace,
                        )
                        secret.metadata.owner_references = [
                            k8s.V1OwnerReference(
                                api_version="v1",
                                kind="Pod",
                                name=pod.metadata.name,
                                uid=pod.metadata.uid,
                            )
                        ]
                        client.patch_namespaced_secret(
                            name=secret_name,
                            namespace=secret_namespace,
                            body=secret,
                        )

                self.callbacks = DuckDBPodOperatorCallback
            else:
                self.log.warning(
                    "The version of the Kubernetes provider is too old to support secret ownership, "
                    "please upgrade to a version >= 7.14.0 or manually clean up the secrets periodically."
                )
        return super().execute(context)
