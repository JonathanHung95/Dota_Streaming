# Dota_Streaming
Project to stream data from the Dota API~

## Dota 2 API calls

Data can be obtained from the Dota 2 API using HTTP requests.  The API will return a JSON object.

### Get Match History

This request will return a JSON object with the last <matches_requested> number of matches.  Parameters include:

key: Steam API key to make request
matches_requested: Returns last number of requested matches, up to 100
skill: Skill level of games (1-3)
min_players: Minimum number of players in the game

https://api.steampowered.com/IDOTA2Match_570/GetMatchHistory/V001/?key=API_KEY&matches_requested=10

### Get Match Details

This request returns a JSON object with detailed info of the requested match.  Parameters include:

key: Steam API key to make request
match_id: ID of the match requested

https://api.steampowered.com/IDOTA2Match_570/GetMatchDetails/V001/?match_id=MATCHID&key=APIKEY

### Get Heroes

Returns a JSON object with hero details.

https://api.steampowered.com/IEconDOTA2_570/GetHeroes/v0001/?key=APIKEY

### Get Items

Returns a JSON object with item details.

https://api.steampowered.com/IEconDOTA2_570/GetGameItems/V001/?key=APIKEY&language=LANGCODE