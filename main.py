LOANS = "loans"
BOOKS = "books"
USERS = "users"

NUMBOOKS = 10
NUMLOANS = 10
NUMUSERS = 10

import sqlite3

def main() -> None:
    con = sqlite3.connect("library.db")

    cur = con.cursor()



    ## clean up old tables

    cur.execute(f"DROP TABLE IF EXISTS {USERS};")
    cur.execute(f"DROP TABLE IF EXISTS {LOANS};")
    cur.execute(f"DROP TABLE IF EXISTS {BOOKS};")
    cur.execute(f"DROP VIEW IF EXISTS user_loans")

    ## this section of code deals with creation of the tables
    ## DDL create

    ## create book table

    cur.execute(
        f"""
        create table {BOOKS} (
            {BOOKS}id integer primary key,
            title text not null,
            author text not null,
            esbn text not null,
            copies integer default 1 not null,
            genre text not null,
            unique(esbn) on conflict abort,
            check(copies > 0) on conflict abort
        );
        """
    )

    ## create users table

    cur.execute(
        f"""
        create table {USERS} (
            {USERS}id integer primary key,
            fullname text not null,
            address text not null,
            lates integer default 0 not null,
            email text default null,
            mobile text default null
            );
        """
    )

    ## create loans table

    cur.execute(
        f"""
        create table {LOANS} (
            {LOANS}id integer primary key,
            {BOOKS}id integer not null,
            {USERS}id integer not null,
            duedate date not null,
            foreign key ({BOOKS}id) REFERENCES {BOOKS} ({BOOKS}id),
            foreign key ({USERS}id) REFERENCES {USERS} ({USERS}id)
        );
        """
    )

    ## this section of code deals with creation of data
    ## DML create


    ## populate books
    for i in range(NUMBOOKS):
        cur.execute(
            f"""
            insert into {BOOKS} (title, author, esbn, copies, genre) values (
                'example title {i}',
                'example author {i}',
                '000000000000{i}',
                1,
                'example genre {i}'
            );
            """
        )

    ## populate users
    for i in range(NUMUSERS):
        cur.execute(
            f"""
            insert into {USERS} (fullname, address, lates, email, mobile) values (
                'example name {i}',
                'example adress {i}',
                0,
                'person{i}@gmail.com',
                '000000000000{i}'
            )
            """
        )

    ## populate loans
    for i in range(NUMLOANS):
        cur.execute(
            f"""
            insert into {LOANS} ({BOOKS}id, {USERS}id, duedate) values (
                {i},
                {i},
                {i}
            )
            """
        )


    con.commit()

    ## create view for users and loans

    cur.execute(
        f"""
        create view
        user_loans
        as
        select
            loansid,
            {USERS}.fullname as name,
            {BOOKS}.title as title,
            {USERS}.address as address,
            {USERS}.email as email,
            {LOANS}.duedate as date
        from
            loans
        inner join {USERS} on {USERS}.{USERS}id = {LOANS}.{LOANS}id
        inner join {BOOKS} on {BOOKS}.{BOOKS}id = {LOANS}.{LOANS}id;
        """
    )


    ## append test column to all tables
    ## DDL update

    cur.executescript(
        f"""
        begin;
        alter table {USERS} add column test null;
        alter table {LOANS} add column test null;
        alter table {BOOKS} add column test null;
        commit;
        """
    )

    ## update first item in each database
    ## DML update

    cur.executescript(
        f"""
        begin;
        update {USERS} set fullname = 'update test' where {USERS}id = 1;
        update {LOANS} set duedate = 43110 where {LOANS}id = 1;
        update {BOOKS} set title = "update test" where {BOOKS}id = 1;
        commit;
        """
    )

    ## fetch table structure
    ## DDL read

    print("\n--------------DB structure--------------\n")

    ## fetch books structure
    cur.execute(f"SELECT sql FROM sqlite_schema WHERE name = '{BOOKS}';")
    print(cur.fetchone()[0])
    print()

    ## fetch users structure
    cur.execute(f"SELECT sql FROM sqlite_schema WHERE name = '{USERS}';")
    print(cur.fetchone()[0])
    print()

    ## fetch loans structure
    cur.execute(f"SELECT sql FROM sqlite_schema WHERE name = '{LOANS}';")
    print(cur.fetchone()[0])
    print()

    ## fetch table contents
    ## DML read

    print("\n--------------tables view--------------\n")

    ## fetch table contents for loans table
    print_table(cur, LOANS)

    ## fetch table contents for users table
    print_table(cur, USERS)

    ## fetch table contents for books table
    print_table(cur, BOOKS)

    ## fetch loans, users and books combined view
    print_table(cur, "user_loans")


    ## delete row 1 on each table
    ## DML delete

    ## delete from loans
    cur.execute(f"delete from {LOANS} where {LOANS}id = 1;")

    ## delete from users
    cur.execute(f"delete from {USERS} where {USERS}ID = 1;")

    ## delete from books
    cur.execute(f"delete from {BOOKS} where {BOOKS}id = 1;")

    con.commit()

    ## fetch table contents
    ## DML read

    print("\n--------------tables view--------------\n")

    ## fetch table contents for loans table
    print_table(cur, LOANS)

    ## fetch table contents for users table
    print_table(cur, USERS)

    ## fetch table contents for books table
    print_table(cur, BOOKS)

    print_table(cur, "user_loans")

    ## drop all tables
    ## DDL delete

    ## delete loans table
    cur.execute(f"drop table {LOANS};")

    ## delete users table
    cur.execute(f"drop table {USERS}")

    ## delete books table
    cur.execute(f"drop table {BOOKS}")

    con.commit()


    ## close active connection
    cur.close()

def print_table(cur, tableName) -> None:
    print(f"               {tableName} table               ")


    cur.execute(f"pragma table_info('{tableName}');")
    for i in cur.fetchall():
        print(i[1],end="    ")
    print()

    cur.execute(f"select * from {tableName};")
    for i in cur.fetchall():
        for n in i:
            print("   ", n, "   ", end = "")
        print("\n")
    print("\n")

if __name__ == "__main__":
    main()