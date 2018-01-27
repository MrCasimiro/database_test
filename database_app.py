import psycopg2
import getpass
from random import randint, uniform

user = input("Inserir nome de usuário: ")
password = getpass.getpass("Inserir senha: ")
database = input("Inserir nome do banco de dados: ")


def create_table():

    command = """
        CREATE TABLE tb_customer_account (
            id_customer serial primary key,
            cpf_cnpj varchar(11) not null,
            nm_customer varchar(255) not null,
            is_active integer not null,
            vl_total float not null)
        """
    conn = None
    try:
        conn = psycopg2.connect(database=database,
                                user=user,
                                password=password)
        cur = conn.cursor()
        cur.execute(command)

        cur.close()
        # commit the changes
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def populate():
    filepath = 'names.txt'
    conn = None
    try:
        conn = psycopg2.connect(database=database, user=user,
                                password=password)
        cur = conn.cursor()
        with open(filepath, 'r') as fp:
            line = fp.readline()
            while line:
                name = line.split(' ')[0]
                cpf = ''
                active = randint(0, 1)
                total = round(uniform(0, 5000), 2)

                for count in range(11):
                    cpf = cpf + str(randint(0, 9))
                # print(name + " " + cpf + " " + str(total))

                query = """INSERT INTO tb_customer_account(cpf_cnpj,
                    nm_customer, is_active, vl_total)
                    VALUES(%s, %s, %s, %s);"""
                cur.execute(query, (cpf, name, active, total))
                line = fp.readline()
            conn.commit()
            cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def query():
    conn = None
    try:
        conn = psycopg2.connect(database=database, user=user,
                                password=password)
        cur = conn.cursor()

        query = """ SELECT nm_customer, vl_total
                    FROM tb_customer_account
                    WHERE vl_total > 560 AND id_customer > 1500
                    AND id_customer < 2700 ORDER BY vl_total DESC; """
        cur.execute(query)
        customers = ''
        total = 0
        count = 1
        row = cur.fetchone()

        while row is not None:
            total = total + row[1]
            customers = customers + '\n' + row[0]
            count = count + 1
            row = cur.fetchone()
        cur.close()

        print(total / count)
        print(customers)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':
    create_table()
    populate()
    query()
