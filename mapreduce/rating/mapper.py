def mapper_get_ratings(self,_,line):
    (userID, movieID, rating, timestamp) = line.split('\t')
    yield rating,1