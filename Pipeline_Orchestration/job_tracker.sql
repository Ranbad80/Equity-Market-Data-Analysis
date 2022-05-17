CREATE DATABASE job_tracker;
CREATE TABLE job_tracker_table(
    job_id VARCHAR(40) NOT NULL,
    job_status VARCHAR(40),
    update_time VARCHAR(40),
PRIMARY KEY(job_id)

);
