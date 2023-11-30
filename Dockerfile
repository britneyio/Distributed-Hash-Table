FROM golang:latest

RUN go env -w GO111MODULE=off
ADD main.go /prj5/
ADD objects1.txt /prj5/
ADD objects5.txt /prj5/
ADD objects10.txt /prj5/
ADD objects50.txt /prj5/
ADD objects66.txt /prj5/
ADD objects100.txt /prj5/
ADD objects126.txt /prj5/
WORKDIR /prj5/ 
RUN go build -o main


ENTRYPOINT ["/prj5/main"]