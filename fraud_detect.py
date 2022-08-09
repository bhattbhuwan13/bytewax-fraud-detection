import datetime
import json

from bytewax import Dataflow, inputs, parse, run, run_cluster, spawn_cluster


def load_json(file_name="mydata.json"):
    with open(file_name, "r") as open_file:
        for row in json.load(open_file):
            value = (
                str(row["id"]),
                (
                    row["Transaction_time"],
                    int(row["Amount_spent"]),
                ),
            )

            yield row["id"], value  # Change the form of value to key, value


class FraudTransaction:
    def __init__(self):
        self.previous_transaction_value = float("inf")
        self.current_transaction_value = None
        self.flagged_items = []

    def detect_fraud(self, data):

        self.current_transaction_value = data[1]
        if (
            self.current_transaction_value
            >= 1.5 * self.previous_transaction_value
        ):
            self.flagged_items.append(data)
        self.previous_transaction_value = self.current_transaction_value
        return self, self.flagged_items


flow = Dataflow()

flow.stateful_map(
    "fraud", lambda key: FraudTransaction(), FraudTransaction.detect_fraud
)
flow.capture()

for epoch, item in run(flow, load_json()):
    print(item)
