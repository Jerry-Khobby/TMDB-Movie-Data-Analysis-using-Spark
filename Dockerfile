FROM apache/spark:3.5.1-scala2.12-java17-python3-ubuntu

USER root

WORKDIR /tmdbmovies/app

COPY requirements.txt .

RUN pip3 install --no-cache-dir -r requirements.txt

COPY . /tmdbmovies/app

RUN chown -R spark:spark /tmdbmovies

USER spark

EXPOSE 4040

CMD ["tail", "-f", "/dev/null"]
