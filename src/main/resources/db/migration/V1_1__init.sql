    create table data_sync_job_metadata (
        dataset_id int,
        rank int,
        primary key (dataset_id)
    );

    create table dataset (
        dataset_id int,
        name varchar(50),
        primary key(dataset_id)
    );

    create table etl_job_configuration (
        job_id int,
        is_active boolean,
        primary key (job_id)
    );

    create table job (
        job_id int,
        job_name varchar(100),
        primary key (job_id)
    );

    insert into dataset values (1, 'reference');
    insert into dataset values (2, 'price');
    insert into dataset values (3, 'trade');

    insert into data_sync_job_metadata values (1, 1);
    insert into data_sync_job_metadata values (2, 2);
    insert into data_sync_job_metadata values (3, 3);

    insert into job values (1, 'data-sync');

    insert into etl_job_configuration values (1, true);