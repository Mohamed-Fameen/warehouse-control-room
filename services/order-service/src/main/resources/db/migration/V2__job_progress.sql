-- Job progress from dispatchers (Kafka job.events).
alter table pick_jobs
    add column robot_id text,              -- robot on the current attempt
    add column attempt  int  not null default 1,
    add column reason   text;              -- why the last attempt failed, or why the job was dead-lettered
