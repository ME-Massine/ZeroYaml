-- A run groups the Jobs inferred for one repository revision. Step and Job
-- lifecycle state live in job; the run identity stays stable across redelivery.
CREATE TABLE pipeline_run (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline_id UUID NOT NULL,
    repository_location TEXT NOT NULL,
    revision TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT pipeline_run_source_key UNIQUE (pipeline_id, repository_location, revision)
);

CREATE INDEX pipeline_run_repository_idx ON pipeline_run (repository_location, revision);

CREATE TABLE job (
    id UUID PRIMARY KEY,
    pipeline_run_id BIGINT REFERENCES pipeline_run(id),
    step_name VARCHAR(64),
    step_position INTEGER,
    repository_location TEXT NOT NULL,
    revision TEXT NOT NULL,
    command_json JSONB NOT NULL,
    working_directory TEXT NOT NULL,
    status VARCHAR(16) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    queued_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    runner_id TEXT,
    runner_instance_id TEXT,
    failure_code TEXT,
    failure_message TEXT,
    exit_code INTEGER,
    CONSTRAINT job_pipeline_link_check CHECK (
        (pipeline_run_id IS NULL AND step_name IS NULL AND step_position IS NULL)
        OR (pipeline_run_id IS NOT NULL AND step_name IS NOT NULL AND step_position > 0)
    ),
    CONSTRAINT job_status_check CHECK (status IN ('CREATED', 'QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED')),
    CONSTRAINT job_lifecycle_check CHECK (
        (status = 'CREATED' AND queued_at IS NULL AND started_at IS NULL AND completed_at IS NULL)
        OR (status = 'QUEUED' AND queued_at IS NOT NULL AND started_at IS NULL AND completed_at IS NULL)
        OR (status = 'RUNNING' AND queued_at IS NOT NULL AND started_at IS NOT NULL AND completed_at IS NULL)
        OR (status IN ('SUCCEEDED', 'FAILED') AND queued_at IS NOT NULL AND started_at IS NOT NULL AND completed_at IS NOT NULL)
        OR (status = 'CANCELLED' AND completed_at IS NOT NULL)
    ),
    CONSTRAINT job_assignment_check CHECK (
        (started_at IS NULL AND runner_id IS NULL AND runner_instance_id IS NULL)
        OR (started_at IS NOT NULL AND runner_id IS NOT NULL AND runner_instance_id IS NOT NULL)
    ),
    CONSTRAINT job_failure_check CHECK (
        (status = 'FAILED' AND failure_code IS NOT NULL AND failure_message IS NOT NULL)
        OR (status = 'SUCCEEDED' AND failure_code IS NULL AND failure_message IS NULL)
        OR (status NOT IN ('FAILED', 'SUCCEEDED') AND failure_code IS NULL AND failure_message IS NULL AND exit_code IS NULL)
    ),
    CONSTRAINT job_time_order_check CHECK (
        (queued_at IS NULL OR queued_at >= created_at)
        AND (started_at IS NULL OR started_at >= created_at)
        AND (completed_at IS NULL OR completed_at >= created_at)
        AND (started_at IS NULL OR completed_at IS NULL OR completed_at >= started_at)
    )
);

CREATE INDEX job_pipeline_run_idx ON job (pipeline_run_id, step_position);
