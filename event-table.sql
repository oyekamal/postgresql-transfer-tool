-- Drop the table first
DROP TABLE IF EXISTS omar.analytics_analyticsevent;

-- Recreate based on the Django model
CREATE TABLE omar.analytics_analyticsevent (
    id BIGSERIAL PRIMARY KEY,
    created TIMESTAMPTZ NOT NULL,
    modified TIMESTAMPTZ NOT NULL,
    name VARCHAR(255) NOT NULL,
    sent_at TIMESTAMPTZ NOT NULL,
    organization_id BIGINT, -- ForeignKey to Organization (nullable)
    school_id BIGINT,       -- ForeignKey to School (nullable)
    user_id BIGINT NOT NULL, -- ForeignKey to User (NOT NULL because CASCADE)
    user_ip VARCHAR(255),
    identify JSONB,
    properties JSONB,
    synced_with_posthog BOOLEAN NOT NULL DEFAULT FALSE,
    last_local_modified_at TIMESTAMPTZ
);

-- (Optional) You can also add FOREIGN KEY constraints if you want:
-- (Assuming omar.organization, omar.school, and omar.auth_user tables exist)
-- Otherwise skip these constraints.

-- ALTER TABLE omar.analytics_analyticsevent
-- ADD CONSTRAINT fk_analytics_organization FOREIGN KEY (organization_id) REFERENCES omar.organization(id) ON DELETE SET NULL;

-- ALTER TABLE omar.analytics_analyticsevent
-- ADD CONSTRAINT fk_analytics_school FOREIGN KEY (school_id) REFERENCES omar.school(id) ON DELETE SET NULL;

-- ALTER TABLE omar.analytics_analyticsevent
-- ADD CONSTRAINT fk_analytics_user FOREIGN KEY (user_id) REFERENCES omar.auth_user(id) ON DELETE CASCADE;
