# icecat-demo

Plaese install latest python version (3.10) in  your machine.

https://www.python.org/downloads/

and then install Make in your machine

rename .env.sample to .env

Fill out credentials in .env file.

You need to put the json file for gcp service account that we provide into cert folder

Next, run the following command

make install

make run

In your browser, you go to http://localhost:8000/docs

 You will see Swagger api interface

 where you can test catalogs downloading and importing.

That's it, You will get there!

# For docker
build: docker build -t myimage .

run : docker run -d --name mycontainer -p 80:80 myimage

You can go to http://localhost/docs
