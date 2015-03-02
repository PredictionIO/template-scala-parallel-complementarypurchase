"""
Import sample data for complimentary purchase engine
"""

import predictionio
import argparse
import random
import uuid
from datetime import datetime
from datetime import timedelta
import pytz

SEED = 3

def import_events(client):
  random.seed(SEED)
  count = 0
  print client.get_status()
  print "Importing data..."

  # generate 10 users, with user ids u1,u2,....,u10
  user_ids = ["u%s" % i for i in range(1, 10+1)]

  # randomly generate 4 frequent item set
  item_sets = {}
  for i in range(1, 4+1):
    # each set contain 2 to 4 items
    iids = range(1, random.randint(2, 4)+1)
    item_sets[i] = ["s%si%s" % (i, j) for j in iids]

  # plus 20 other items not in any set.
  other_items = ["i%s" % i for i in range(1, 20+1)]

  # 3 popular item every one buy
  pop_items = ["p%s" % i for i in range(1,3+1)]

  # each user have 5 basket purchases:
  for uid in user_ids:
    base_time = datetime(
      year = 2014,
      month = 10,
      day = random.randint(1,31),
      hour = 15,
      minute = 39,
      second = 45,
      microsecond = 618000,
      tzinfo = pytz.timezone('US/Pacific'))
    seconds = 0
    for basket in range(0, 5):
      # may or may not some random item
      if (random.choice([True, False])):
        buy_items = random.sample(other_items, random.randint(1, 3))
        for iid in buy_items:
          event_time = base_time + timedelta(seconds=seconds, days=basket)
          print "User", uid, "buys item", iid, "at", event_time
          client.create_event(
            event = "buy",
            entity_type = "user",
            entity_id = uid,
            target_entity_type = "item",
            target_entity_id = iid,
            event_time = event_time
          )
          seconds += 10
          count += 1

      # always buy one popular item
      buy_items = random.sample(pop_items, 1)
      for iid in buy_items:
        event_time = base_time + timedelta(seconds=seconds, days=basket)
        print "User", uid, "buys item", iid, "at", event_time
        client.create_event(
          event = "buy",
          entity_type = "user",
          entity_id = uid,
          target_entity_type = "item",
          target_entity_id = iid,
          event_time = event_time
        )
        seconds += 10
        count += 1

      # always buy some something from one of the item set
      s = item_sets[random.choice(item_sets.keys())]
      buy_items = random.sample(s, random.randint(2, len(s)))
      for iid in buy_items:
        event_time = base_time + timedelta(seconds=seconds, days=basket)
        print "User", uid, "buys item", iid, "at", event_time
        client.create_event(
          event = "buy",
          entity_type = "user",
          entity_id = uid,
          target_entity_type = "item",
          target_entity_id = iid,
          event_time = event_time
        )
        seconds += 10
        count += 1

  print "%s events are imported." % count

if __name__ == '__main__':
  parser = argparse.ArgumentParser(
    description="Import sample data for similar product engine")
  parser.add_argument('--access_key', default='invald_access_key')
  parser.add_argument('--url', default="http://localhost:7070")

  args = parser.parse_args()
  print args

  client = predictionio.EventClient(
    access_key=args.access_key,
    url=args.url,
    threads=4,
    qsize=100)
  import_events(client)
