CREATE TABLE IF NOT EXISTS moderation_decisions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    image_id UUID NOT NULL,
    inference_mode VARCHAR(20) NOT NULL,
    moderation_probability FLOAT NOT NULL,
    recommendation VARCHAR(10) NOT NULL,
    trigger_type VARCHAR(30),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_moderation_image ON moderation_decisions(image_id);
CREATE INDEX IF NOT EXISTS idx_moderation_recommendation ON moderation_decisions(recommendation);
