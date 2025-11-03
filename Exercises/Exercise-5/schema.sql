-- drop tables in dependency order
drop table if exists transactions cascade;
drop table if exists products cascade;
drop table if exists accounts cascade;

-- create accounts table
create table accounts (
    customer_id int primary key,
    first_name varchar(100),
    last_name varchar(100),
    address_1 varchar(255),
    address_2 varchar(255),
    city varchar(100),
    state varchar(100),
    zip_code varchar(20),
    join_date date
);

-- create products table
create table products (
    product_id int primary key,
    product_code varchar(50),
    product_description varchar(255)
);

-- create transactions table
create table transactions (
    transaction_id varchar(100) primary key,
    transaction_date date,
    product_id int,
    product_code varchar(50),
    product_description varchar(255),
    quantity int,
    account_id int,
    foreign key (product_id) references products(product_id),
    foreign key (account_id) references accounts(customer_id)
);

-- create indexes for faster joins/queries
create index idx_transactions_account_id on transactions(account_id);
create index idx_transactions_product_id on transactions(product_id);