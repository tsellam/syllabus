TARGET=main
all: $(TARGET)

main: index.html schedule.csv
	git commit -am "updated website"
	git push
	./update.sh 
