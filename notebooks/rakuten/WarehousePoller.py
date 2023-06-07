import snowflake.connector
import threading
import time
import os

class WarehousePoller:
    def __init__(self):
        # self.account = account
        # self.user = user
        # self.password = password
        self.connection = None
        self.polling_thread = None
        self.__polling_on = False
        print("Number of active threads:", threading.active_count())

    def connect(self):
        self.connection = snowflake.connector.connect(
            user=os.environ['SNOWFLAKE_USER'],
            role=os.environ['SNOWFLAKE_ROLE'],
            password=os.environ['SNOWFLAKE_PASSWORD'],
            account=os.environ['SNOWFLAKE_ACCOUNT'],
            warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
            database=os.environ['SNOWFLAKE_DATABASE'],
            schema=os.environ['SNOWFLAKE_SCHEMA'],
            client_session_keep_alive=True
        )


    def disconnect(self):
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    def poll_warehouses(self):
        while (self.__polling_on):
            print("something")
            # if self.connection is not None:
            #     cursor = self.connection.cursor()
            #     cursor.execute("show warehouses")
            #     rows = cursor.fetchall()
            #     print(f"Available warehouses: {rows}")
            #     cursor.close()
            time.sleep(1)

    def start_polling(self):
        self.__polling_on = True
        self.polling_thread = threading.Thread(target=self.poll_warehouses)
        self.polling_thread.start()

    def stop_polling(self):
        print(self.polling_thread)
        self.__polling_on = False
        if self.polling_thread is not None:
            print("joining thread...")
            self.polling_thread.join()
            print("done joining thread...")
            self.polling_thread = None
               
# https://stackoverflow.com/questions/18018033/how-to-stop-a-looping-thread-in-python
# https://stackoverflow.com/questions/43879149/stop-a-thread-flag-vs-event
# https://stackoverflow.com/questions/323972/is-there-any-way-to-kill-a-thread/325528#325528

            
print("hello")

poller = WarehousePoller()
poller.connect()

print("Number of active threads:", threading.active_count())

poller.start_polling()
# Wait a bit to see the output
time.sleep(5)

print("stopping polling...")
poller.stop_polling()
print("disconnecting....")
poller.disconnect()