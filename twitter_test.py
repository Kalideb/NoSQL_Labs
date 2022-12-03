# NOTE: What present in this module
# 1) Login -> validate user credentials
# 2) Sign in -> create user
# 3) Get/Set userId by username methods
# 4) Post twit method
# 5) Fill database with twits from file (file structure represented with rows like [username|message])
# 6) Get twits from particular user by username (inefficient tho)

# TODO: Hashtags, followers

import cassandra.cluster
import redis 
import uuid
import datetime

import faker
import faker.providers

# Random data
faker = faker.Faker()

# Represents some kind of user client
# TODO: Do some code refactoring (maybe...)

class Client():
    # Set necessary variables and connect to databases
    def __init__(self, log=False):
        # Variables
        self.userKeyspace = "users"
        self.userTable = "user"
        self.userRedisHash = "twitterUsers"

        self.twitsKeyspace = "twits"
        self.twitsTables = ["twit", "userTwit"]

        # Connect to Cassandra cluster and set sessions
        self.cluster = cassandra.cluster.Cluster()
        self.userSession = self.cluster.connect(self.userKeyspace)
        self.twitsSession = self.cluster.connect(self.twitsKeyspace)

        # Connect to Redis
        self.userRedisInstance = redis.StrictRedis("localhost", port=6379)
        if log:
            print("[*] Client initialized")

    def getUserId(self, username):
        return client.userRedisInstance.hget(client.userRedisHash, username).decode("utf-8")

    def setUserId(self, username, userId = uuid.uuid4(), /, overwrite=False):
        if getUserId(username) != None:
            if overwrite == True:
                client.userRedisInstance.hset(client.userRedisHash, username, userId)
                return True
        else:
            client.userRedisInstance.hset(client.userRedisHash, username, userId)
            return True
        return False

    
    # Simple login: get userId by username in Redis and validate it with Cassandra user entry
    def login(self, username, password):
        userId = self.getUserId(username)

        self.curUserId = userId
        self.username = username
        self.password = password

        if userId != None:
            status = False
            for user in client.userSession.execute(f"SELECT * FROM user WHERE id={userId}"):
                if user.password == password:
                    print("[*] Login succesfully!")
                    status = True
                    break
        
            if not status:
                print("[!] Failed to login. Wrong password")
        else:
            print("User not found in Redis cache")

    def postTwit(self, userId, username, message):
        if message == None:
            return

        todayYear = datetime.date.today().year
        twitId = uuid.uuid4()
        self.twitsSession.execute(f"INSERT INTO {self.twitsTables[0]} "
                                  f"(id, authorId, message, tstamp, year) "
                                  f"VALUES ({twitId}, {uuid.UUID(userId)}, '{message}', toTimestamp(now()), {todayYear});")
        self.twitsSession.execute(f"INSERT INTO {self.twitsTables[1]} "
                                  f"(twitId, authorId, authorUsername, year, tstamp) "
                                  f"VALUES ({twitId}, {userId}, '{username}', {todayYear}, toTimestamp(now()));")
        print("[*] Twit posted")
    def signin(self, username, password):
        # Check if user exists
        if self.getUserId(username) == None:
            userId = uuid.uuid4()
            self.userSession.execute(f"INSERT INTO {self.userTable} "
                                     f"(id, username, password) VALUES "
                                     f"({userId}, '{username}', '{password}');")
            self.userRedisInstance.hset(self.userRedisHash, username, str(userId))
            return True
        else:
            return False


    # Accepts file with normalized tweet dataset in following format:
    # EXAMPLE: username|message

    def fillTwits(self, twitDataset):
        users, usersId = {}, {}
        with open(twitDataset) as f:
            for line in f:
                username, message = tuple(line.split("|"))
                self.signin(username, faker.password())
                # Redis contains data in binary format
                userId = self.userRedisInstance.hget(self.userRedisHash, username).decode("utf-8")
                self.postTwit(userId, username, message)
                print("[*] Twit posted")

    # TODO: Get twits from particular user
    # 1) Get userId from users:user
    # 2) Get twitIds from twits:userTwit by userId (authorId)
    # 3) Get twit messages from twits:twit by twitId

    # NOTE: Ineffective implementation, it gets all twits for all time
    # In reality better to load more twits when previous pack was read e.g "load more twits"

    # Gets all twits in chronological order (twits are ordered by timestamp e.g clustering key)
    def getTwits(self, username, lastYear, /, getAmount = False, amount = 1):
        userId = self.getUserId(username)

        currentYear = datetime.date.today().year
        twitIds = []
        exitLoop = False

        # Collect twitIds from user
        while not exitLoop:
            twitIdRows = self.twitsSession.execute(f"SELECT * FROM {self.twitsTables[1]} WHERE authorId={userId} AND year={currentYear};")
            # Get twits from previous year if current is empty
            if twitIdRows == None:
                currentYear -= 1
            else:
                for row in twitIdRows:
                    if amount == 0 and getAmount:
                        break
                    twitIds.append(row.twitid)
                    amount -= 1
            changedYear = False

            if (amount == 0 and getAmount) or (currentYear < lastYear):
                exitLoop = True

        messages = []
        # Get user messages by twitId
        for twitId in twitIds:
            twitRow = self.twitsSession.execute(f"SELECT * FROM {self.twitsTables[0]} WHERE id={twitId};")
            messages.append(twitRow.one().message)

        del twitIdRows
        del twitIds

        return messages

client = Client()
# Example 1
# Get twits from user
username = "ANAAISLEC"
messages = client.getTwits(username, 2015, getAmount=True, amount=100)
for msg in messages:
    print(f"@{username}: {msg}")
    print("=" * 50)

# Example 2
# Fill twits from normalized file (only username and message rows)
#
# filePath = "some/path/example.txt"
# client.fillTwits(filePath)
