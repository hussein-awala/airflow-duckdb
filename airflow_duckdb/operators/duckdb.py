from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import kubernetes.client as k8s
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

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
    template_ext = ("sql", "sql.j2")

    def __init__(
        self,
        query: str,
        s3_fs_config: S3FSConfig | None = None,
        image: str = "airflow_duckdb:latest",
        name: str = "duckdb",
        **kwargs
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

    def execute(self, context):
        """Execute the operator."""
        # split the query by semicolon
        parsed_query = self.query.rstrip(";").split(";")
        # create a list of pre-execution commands
        pre_execution = []
        if self.do_xcom_push:
            # if xcom_push is enabled, we will save the last SELECT statement as a JSON file
            last_query = parsed_query[-1]
            if last_query.strip().upper().startswith("SELECT"):
                last_query = f"COPY ({last_query}) TO '/airflow/xcom/return.json' (FORMAT JSON, ARRAY true);"
            parsed_query = parsed_query[:-1] + [last_query]
        if self.s3_fs_config:
            # if S3 filesystem is configured, we will load the S3 filesystem and set the configurations
            pre_execution.extend([
                "LOAD httpfs",
                "LOAD aws",
                "CALL load_aws_credentials()",
                "SET s3_url_style='path'",
            ])
            # set the S3 filesystem configurations from the S3FSConfig (useful for local development with MinIO)
            if self.s3_fs_config.endpoint:
                pre_execution.append(f"SET s3_endpoint = '{self.s3_fs_config.endpoint}'")
            pre_execution.append(f"SET s3_use_ssl = {'true' if self.s3_fs_config.use_ssl else 'false'}")
            # TODO: move these configurations to a secret
            if self.s3_fs_config.access_key_id:
                self.env_vars.append(k8s.V1EnvVar(name="AWS_ACCESS_KEY_ID", value=self.s3_fs_config.access_key_id))
            if self.s3_fs_config.secret_access_key:
                self.env_vars.append(k8s.V1EnvVar(name="AWS_SECRET_ACCESS_KEY", value=self.s3_fs_config.secret_access_key))
            if self.s3_fs_config.region:
                self.env_vars.append(k8s.V1EnvVar(name="AWS_REGION", value=self.s3_fs_config.region))
        # reconstruct the query
        self.query = ";\n".join(pre_execution + parsed_query)
        self.arguments = [
            "-s",
            self.query,
        ]
        return super().execute(context)
