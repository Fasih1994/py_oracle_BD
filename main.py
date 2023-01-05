"""  
TABLE SCHEMA

        ID PRODUCT_ID	     QTY      PRICE
---------- ---------- ---------- ----------
	 1	             22	       2      30.32
"""

import random
from dataclasses import dataclass, field
import oracledb
import config as cfg
from faker import Faker
from time import time
import time
import datetime

faker = Faker()


skus = "Smart Phone,TV,Watches,Tablet & Laptops,Accesories,Routers,Gaming,Audio Device,Home Device,Wifi Solutions, Smart TV".split(',')
base = datetime.datetime.today()
date_list = [base - datetime.timedelta(days=x) for x in range(360)]

@dataclass
class Purchase:
  username:str = field(default_factory=faker.user_name)
  currency:str = field(default_factory=faker.currency_code)
  purchase_value:float = field(default_factory=lambda: round(random.uniform(200,100000),2))
  country:str = field(default_factory=faker.country)


@dataclass
class Order:
  qty:int = field(default_factory=lambda: random.randint(1,20))
  sku_name:str = field(default_factory=lambda: random.choice(skus))



def insert_into(query, data):
  try:
    # establish a new connection
    with oracledb.connect(user=cfg.username,
                        password=cfg.password,
                        dsn=cfg.dsn,
                        encoding=cfg.encoding) as connection:
        # create a cursor
        with connection.cursor() as cursor:
          # execute the insert statement
          cursor.execute(query, data)
          # commit work
          connection.commit()
  except oracledb.Error as error:
    print('Error occurred:')
    print(str(error))

def insert_order(id:int = None, sku_name:str = None, qty:int = None, order_value:float = None, ts:datetime.datetime=None):
  # construct an insert statement that add a new row to the billing_headers table
  query = f"INSERT INTO orders(TRANSACTION_ID, QTY, SKU_NAME, ORDER_VALUE, ORDERED_AT) values(:tnx_id, :qty, :sku_name, :order_value, :ordered_at)"
  data = {
    "tnx_id":id,
    "qty":qty,
    "sku_name":sku_name,
    "order_value":order_value,
    'ordered_at':ts
  }
  insert_into(query=query,data=data)
  



def insert_purchase(
  id: int= None,
  username: str=None,
  currency: str=None,
  country: str=None,
  purchase_value:float=None,
  purchase_time: datetime.datetime=None
  ):

  query = f"INSERT INTO purchase(TRANSACTION_ID, username, currency, purchase_value, country, PURCHASE_TIME) values(:id, :username, :currency, :p_v, :country, :purchase_time)"
  data = {
    "id":id,
    "username":username, 
    "currency":currency, 
    "p_v": purchase_value,
    "country":country, 
    "purchase_time":purchase_time
  }
  insert_into(query=query, data=data)



if __name__=="__main__":
  with open('/home/fasih/python_customer/transaction_id.txt','r') as f:
    trnx_id = f.read()
    if not trnx_id:
      trnx_id = 1
    else:
      trnx_id = int(trnx_id)
  
  for i in range(19):
    date = random.choice(date_list)
    p = Purchase()
    o = Order()
    print(p)
    print(o)
    insert_order(
      id=trnx_id,
      sku_name=o.sku_name,
      qty=o.qty,
      order_value=p.purchase_value * 0.8,
      ts=date
    )

    insert_purchase(
      id=trnx_id,
      username=p.username,
      currency=p.currency,
      country=p.country,
      purchase_value=p.purchase_value,
      purchase_time=date
    )

    trnx_id+=1
    time.sleep(3)

  with open('/home/fasih/python_customer/transaction_id.txt','w') as f:
    f.write(str(trnx_id))
