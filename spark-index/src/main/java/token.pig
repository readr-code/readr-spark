/*
 * Generates the following files:
 * sentence : { sentenceID, documentID, offsetBegin, offsetEnd }
 * //sentenceText : { sentenceID, text }
 * //sentenceToken : { sentenceID, characterOffsets }
 * sentenceTextToken : { sentenceID, text, tokenCharOffsets }
 * token2name : { tokenID, name }
 * tokenInst2basic : { tokenID, sentenceID, tokenOffset }
 * docToken : { documentID, tokenCharOffsets }
 */

DEFINE prependGlobalRowNumber1(A) RETURNS C {
	B = GROUP $A ALL;
	$C = FOREACH B {
         BS = ORDER $1 BY documentID, sentNum;
         GENERATE flatten(com.readr.hadoop.pig.PrependRowNumber(BS)); 
      } 
};

-- read sequence files
protobufToken = loadProtobufToken('$dir/doc.Token');
protobufSentence = loadProtobufSentence('$dir/doc.Sentence');

--Z = FOREACH protobufToken GENERATE $0 as documentID, 


-- join protobufs by documentID
A = JOIN protobufToken BY documentID, protobufSentence BY documentID USING 'merge';
B = FOREACH A GENERATE $0 as documentID, flatten(com.readr.hadoop.pig.JoinTokenAndSentence($1.$1,$3.$1));
BZ = FOREACH B GENERATE documentID, $1 AS sentNum, $2, $3, $4, $5, $6, $7, $8, $9;
--ZS = ORDER BZ BY documentID, sentNum;

-- global sentenceIDs
C = prependGlobalRowNumber1(BZ);

--STORE C INTO '$dir/db/test';

-- we now have the following schema:
-- {sentenceID,documentID,sentNum,tokens,sentenceTokenized,text,tokenOffsets,offsetBegin,offsetEnd,tokenBegin,tokenEnd}


-- generate sentenceTokenized
D = FOREACH C GENERATE $0 as sentenceID, $4 as sentenceTokenized;

-- generate sentenceTextToken
D2 = FOREACH C GENERATE $0 as sentenceID, com.readr.hadoop.pig.Escape($5) as text, $6 as tokenOffsets;

-- generate sentence
D3 = FOREACH C GENERATE $0 as sentenceID, $1 as documentID, $2 as sentNum, $7 as offsetBegin, $8 as offsetEnd, $9 as tokenBegin, $10 as tokenEnd;

-- tokens
E = FOREACH C GENERATE $0 as sentenceID, com.readr.hadoop.pig.PrependRowNumber($3) as tokens;
F = FOREACH E GENERATE $0 as sentenceID, FLATTEN($1);

-- generate (global) tokenIDs
G = FOREACH F GENERATE $2;
H = DISTINCT G;
I = prependGlobalRowNumber(H);

-- replace token by tokenID
J = JOIN F BY $2, I BY $1 USING 'replicated';
K = FOREACH J GENERATE $3 AS tokenID, $0 AS sentenceID, $1 AS offset;

-- store
STORE I INTO '$dir/db/token2name';
STORE D INTO '$dir/db/sentenceTokenized';
STORE K INTO '$dir/db/tokenInst2basic';
STORE D2 INTO '$dir/db/sentenceTextToken';
STORE D3 INTO '$dir/db/sentence';
