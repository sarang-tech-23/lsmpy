from lsm import LSMTree
import random
import string

def main():
    db = LSMTree()
    for i in range(1000):
        k = s = "".join(random.choices(string.ascii_lowercase, k=random.randint(3, 5)))
        v = s = "".join(random.choices(string.ascii_lowercase, k=random.randint(3, 5)))

        db.add(k, v)
        
    # db.print_levels()

if __name__ == "__main__":
    main()