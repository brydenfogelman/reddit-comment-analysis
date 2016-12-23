#bzip2 -d reddit_database.dz2

N=17000000
f=open("reddit_database")
f2=open("new_database", "w+")
for i in range(N):
    line=f.next().strip()
    f2.write(line)
f.close()
f2.close()
