from crawling import kospi

while True:
    action = input("choose action. 1: Crwal KOSPI\n")
    if action == "1":
        kospi.get()
