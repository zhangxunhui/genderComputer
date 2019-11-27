from genderComputer import GenderComputer
import os, warnings, sys, MySQLdb, logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


gc = GenderComputer('./nameLists')


def create_table(cur, tableName):
    # whether the table exists
    try:
        cur.execute("select max(id) from {}".format(tableName))
        cur.fetchone()
        exists = True
    except Exception as e:
        exists = False
    if exists == False:
        sql = "CREATE TABLE `" + tableName + "` (" \
                  "`id` int(11) NOT NULL AUTO_INCREMENT, " \
                  "`user_id` int(11) NOT NULL, " \
                  "`gender` varchar(255) DEFAULT NULL, " \
                  "PRIMARY KEY (`id`), " \
                  "KEY `user_id` (`user_id`) USING BTREE" \
              ") ENGINE=InnoDB DEFAULT CHARSET=utf8;"
        cur.execute(sql)

# execute a list of names
db = MySQLdb.connect(host='localhost',
                user='root',
                passwd='111111',
                db='ght_msr_2014',
                autocommit=False)
cur = db.cursor()

# create user_genders table
create_table(cur, "gender_genderComputer")

# read username from ghtorrent
usernameDict = {}
cur.execute("select login, name from users_private")
items = cur.fetchall()
for item in items:
    login = item[0]
    name = item[1]
    usernameDict[login] = name
logging.info("finish reading table users_private")

# read users table from ghtorrent
cur.execute("select max(user_id) from gender_genderComputer")
max_user_id = cur.fetchone()
if max_user_id[0] is None:
    max_user_id = 0
else:
    max_user_id = max_user_id[0]
print max_user_id
cur.execute("select id, login, location "
            "from users "
            "where id > %s", (max_user_id,))
users = cur.fetchall()
for user in users:
    id = user[0]
    login = user[1]
    if usernameDict.has_key(login) == False:
        name = None
    else:
        name = usernameDict[login]

    if name is None or len(name.strip()) == 0:
        gender = None
        cur.execute("insert into gender_genderComputer (user_id, gender) values (%s, %s)", (id, None))
    else:
        location = user[2]
        try:
            gender = gc.resolveGender(name, location)
            cur.execute("insert into gender_genderComputer (user_id, gender) values (%s, %s)", (id, gender))
        except Exception as e:
            # there are some special characters that cannot be regarded
            cur.execute("insert into gender_genderComputer (user_id, gender) values (%s, %s)", (id, None))
    logging.info("user %d: %s; %s - %s" % (id, name, location, gender))
    if id % 10000 == 0:
        db.commit()
db.commit()