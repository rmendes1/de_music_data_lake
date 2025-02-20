CREATE TABLE kafka_offsets (
    topic VARCHAR(255) NOT NULL,
    "partition" INT NOT NULL,
    "offset" BIGINT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (topic, "partition")
);