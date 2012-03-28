genhtml: genhtml.c
	gcc genhtml.c -lm -o genhtml

parsehtml: parsehtml.c
	gcc -lm -lcrypto hashtable.c hashtable_itr.c parsehtml.c -o parsehtml
	
run: genhtml
	rm -rf docs
	mkdir docs
	./genhtml
