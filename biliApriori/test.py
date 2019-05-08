def is_synonymous(strings1, strings2):
    x1=[]
    synonyms = [
        ("beautiful", "pretty"),
        ("mom", "mommy"),
        ("quite", "very")
    ]
    str1 = strings1.split(' ')
    str2 = strings2.split(' ')
    for s,(a,b) in enumerate(zip(str1,str2)):
        if a==b:
            x1.append(a)
            continue
        for word in synonyms:
            if a in word and b in word:
                x1.append(a)
                continue
    return x1==str1
pass
if __name__ == '__main__':
    x1=is_synonymous("My mommy is quite pretty","My mom is very beautiful")
    print(x1)