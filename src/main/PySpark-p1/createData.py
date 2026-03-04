#Had to generate this using ChatGPT because I didn't know how to do it!
#Generated the data fine, will be included in the report, That I used GPT for this script
#
import random

NUM_RECORDS = 1000000
TABLE_COUNT = 50000

# Open all three files for writing
with open("Meta-Event.txt", "w") as f_meta, \
        open("Meta-Event-No-Disclosure.txt", "w") as f_no_disc, \
        open("Reported-Illnesses.txt", "w") as f_rep:

    for i in range(1, NUM_RECORDS + 1):
        name = f"Person_{i}"
        table = f"T{random.randint(1, TABLE_COUNT)}"

        # 5% chance a person is sick
        test = "sick" if random.random() < 0.05 else "not-sick"

        # 1. Meta-Event dataset
        f_meta.write(f"{i},{name},{table},{test}\n")

        # 2. Meta-Event-No-Disclosure dataset
        f_no_disc.write(f"{i},{name},{table}\n")

        # 3. Reported-Illnesses dataset (Subset of sick people)
        if test == "sick" and random.random() < 0.5:
            f_rep.write(f"{i},{test}\n")

print("Data generation complete.")