

actual_users = set()
expected_users = set()

with open("log/anomalies.txt") as actual_file:
    lines = actual_file.readlines()
    for user_string in lines:
        user_id = user_string.split(",")[1][:-1]
        actual_users.add(user_id)

with open("expected-anomalies.txt") as expected_file:
    lines = expected_file.readlines()
    for user_string in lines:
        user_id = user_string.split(",")[1][:-1]
        expected_users.add(user_id)

diff = actual_users.difference(expected_users)

if len(diff) == 0:
    print("no difference")
else:
    print(diff)

