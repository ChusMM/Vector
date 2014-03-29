create table dictionary(
tj varchar(700) not null,
fdj integer not null,
primary key(tj));

create table documents(
id integer not null auto_increment,
name varchar(100) not null,
path varchar(500) not null,
primary key(id));

create table posting_file(
di integer not null references documents(id) on delete cascade on update cascade,
tj varchar(700) not null references dictionary(tj) on delete cascade on update cascade,
ftij integer not null,
primary key(di, tj));