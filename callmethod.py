import sys
import re

class printHello:
    def print_hello(self):
        return "Hello, world"

  
    def main_test(self):
        print("testing----")
        myObj = printHello()
        print(myObj)
        func = getattr(myObj,"print_hello")
        print(func())


def main():
    title = "Pacific Rim: Uprising"
    #title = re.sub(r':', '', title)

    obj = re.match(r':',title)
    if obj:
        print("matched")

    line = "Cats are smarter than dogs";

    matchObj = re.search( r'd', line, re.M|re.I)
    if matchObj:
        print("match --> matchObj.group() : ", matchObj.group())    

    print(title)

    #obj = printHello()
    #obj.main_test()

if __name__ == '__main__':
    main()
    