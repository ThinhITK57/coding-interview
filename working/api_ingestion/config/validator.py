# config/validator.py

from . import IngestionConfig


class ConfigValidator:

    def validate(
        self,
        config: IngestionConfig
    ):

        self._validate_api(config)

        self._validate_job(config)

        return config

    def _validate_api(
        self,
        config: IngestionConfig
    ):

        api = config.api

        if not api.name:
            raise ValueError(
                "API name cannot be empty"
            )

        if not api.base_url:
            raise ValueError(
                "API base_url cannot be empty"
            )

        if not api.base_url.startswith(
            ("http://", "https://")
        ):
            raise ValueError(
                "API base_url must start with "
                "http:// or https://"
            )

        if not api.endpoints:
            raise ValueError(
                "At least one endpoint "
                "must be configured"
            )

        endpoint_names = set()

        for endpoint in api.endpoints:

            if endpoint.name in endpoint_names:
                raise ValueError(
                    f"Duplicate endpoint: "
                    f"{endpoint.name}"
                )

            endpoint_names.add(
                endpoint.name
            )

            self._validate_endpoint(
                endpoint
            )

    def _validate_endpoint(
        self,
        endpoint
    ):

        if not endpoint.path:
            raise ValueError(
                f"Endpoint '{endpoint.name}' "
                f"has empty path"
            )

        if endpoint.timeout_seconds <= 0:
            raise ValueError(
                f"Endpoint '{endpoint.name}' "
                f"timeout must be > 0"
            )

        if endpoint.pagination:

            pagination = endpoint.pagination

            if pagination.page_size <= 0:
                raise ValueError(
                    f"Endpoint '{endpoint.name}' "
                    f"page_size must be > 0"
                )

            if (
                pagination.max_pages is not None
                and pagination.max_pages <= 0
            ):
                raise ValueError(
                    f"Endpoint '{endpoint.name}' "
                    f"max_pages must be > 0"
                )

        self._validate_rate_limit(endpoint)

    def _validate_rate_limit(
        self,
        endpoint
    ):

        rate_limit = endpoint.rate_limit

        if rate_limit is None:
            return

        if (
            rate_limit.requests_per_minute
            is not None
            and rate_limit.requests_per_minute <= 0
        ):
            raise ValueError(
                "requests_per_minute "
                "must be > 0"
            )

        if (
            rate_limit.requests_per_day
            is not None
            and rate_limit.requests_per_day <= 0
        ):
            raise ValueError(
                "requests_per_day "
                "must be > 0"
            )

    def _validate_checkpoint(
        self,
        checkpoint
    ):

        if not checkpoint.enabled:
            return

        if not checkpoint.store_type:
            raise ValueError(
                "checkpoint.store_type "
                "cannot be empty when checkpoint "
                "is enabled"
            )

        supported_store_types = {
            "file",
            "json",
            "database"
        }

        if checkpoint.store_type not in supported_store_types:
            raise ValueError(
                f"Unsupported checkpoint store type: "
                f"{checkpoint.store_type}. "
                f"Supported values: "
                f"{supported_store_types}"
            )

        if not checkpoint.path:
            raise ValueError(
                "checkpoint.path is required "
                "when checkpoint is enabled"
            )

    def _validate_job(
        self,
        config: IngestionConfig
    ):

        job = config.job

        if not job.job_name:
            raise ValueError(
                "job_name cannot be empty"
            )

        if not job.source:
            raise ValueError(
                "job source cannot be empty"
            )

        if not job.endpoint:
            raise ValueError(
                "job endpoint cannot be empty"
            )

        endpoint_names = {
            endpoint.name
            for endpoint in config.api.endpoints
        }

        if job.endpoint not in endpoint_names:
            raise ValueError(
                f"Job endpoint "
                f"'{job.endpoint}' "
                f"does not exist"
            )

        self._validate_extraction(
            job.extraction
        )

        self._validate_checkpoint(
            job.checkpoint
        )

        self._validate_storage(
            job.storage
        )

    def _validate_extraction(
        self,
        extraction
    ):

        if extraction.batch_size <= 0:
            raise ValueError(
                "batch_size must be > 0"
            )

        if (
            extraction.max_records is not None
            and extraction.max_records <= 0
        ):
            raise ValueError(
                "max_records must be > 0"
            )

        if (
            extraction.max_pages is not None
            and extraction.max_pages <= 0
        ):
            raise ValueError(
                "max_pages must be > 0"
            )

        if (
            extraction.max_runtime_seconds
            is not None
            and extraction.max_runtime_seconds <= 0
        ):
            raise ValueError(
                "max_runtime_seconds "
                "must be > 0"
            )

    def _validate_storage(
        self,
        storage
    ):

        if not storage.path:
            raise ValueError(
                "Storage path cannot be empty"
            )

        if not storage.layer:
            raise ValueError(
                "Storage layer cannot be empty"
            )

        if not storage.compression:
            raise ValueError(
                "Compression cannot be empty"
            )