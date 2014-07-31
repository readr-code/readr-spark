mkdir lib
mkdir download
cd download
wget http://www-nlp.stanford.edu/software/stanford-corenlp-full-2014-06-16.zip
unzip stanford-corenlp-full-2014-06-16.zip
mv stanford-corenlp-full-2014-06-16/stanford*.jar ../lib
cd ..
cd lib
wget http://www-nlp.stanford.edu/software/stanford-srparser-2014-07-01-models.jar
cd ..