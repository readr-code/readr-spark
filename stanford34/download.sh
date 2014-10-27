mkdir lib
mkdir download
cd download
wget http://nlp.stanford.edu/software/stanford-corenlp-full-2014-08-27.zip
unzip stanford-corenlp-full-2014-08-27.zip
mv stanford-corenlp-full-2014-08-27/stanford*.jar ../lib
cd ..
cd lib
wget http://www-nlp.stanford.edu/software/stanford-srparser-2014-08-28-models.jar
cd ..
