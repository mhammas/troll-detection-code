# Troll Detection

Growing evidence points to recurring influence campaigns on social media, often sponsored by state actors aiming to manipulate public opinion on sensitive political topics. Typically, campaigns are performed through instrumented accounts, known as troll accounts; despite their prominence, however, little work has been done to detect these accounts in the wild. 

In this work, we present a detection system for troll accounts. 

# Key Observations 
Our key observation, based on analysis of known Russian-sponsored troll accounts identified by Reddit, is that they show loose coordination, often interacting with each other to further specific narratives. Therefore, troll accounts controlled by the same actor often show similarities that can be leveraged for detection. 

# Detection Algorithm

We train TROLLMAGNIFIER on a set of 335 known troll accounts and run it on a large dataset of Reddit accounts. Our system identifies 1,248 potential troll accounts; we then provide a multi-faceted analysis to corroborate the correctness of our classification. In particular, 66% of the detected accounts show signs of being instrumented by malicious actors (e.g., they were created on the same exact day as a known troll, they have since been suspended by Reddit, etc.). They also discuss similar topics as the known troll accounts and exhibit temporal synchronization in their activity. Overall, we show that using TROLLMAGNIFIER, one can grow the initial knowledge of potential trolls provided by Reddit by over 300%.
