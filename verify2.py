if __name__ == "__main__":
    with open("count.txt") as file:
        for i, line in enumerate(file):
            if int(line) != i:
                print(f"Error at line: {i}")
                break