from wordcloud import WordCloud
import matplotlib.pyplot as plt
from scipy.misc.pilutil import imread

twitter_mask = imread('../res/twitter_mask.png', flatten=True)

# List of words from tweets
text =""
with open('words.txt','r') as f:
    for line in f:
        for word in line.split():
           text+=word+" "

# Create a word cloud		   
wordcloud = WordCloud(background_color='white',
                      width=1800,
                      height=1400,
                      mask=twitter_mask).generate(text)
plt.figure()
plt.imshow(wordcloud, interpolation="bilinear")
plt.axis("off")
plt.margins(x=0, y=0)
plt.show()
