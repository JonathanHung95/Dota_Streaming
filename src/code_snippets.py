# code snippet to replace extra parts of the name in the items json

with open("test.json") as infile, open("test2.json","w") as outfile:
	for line in infile:
		line = line.replace("item_", "")
		outfile.write(line)

# code snippets to convert json to a map like format for scala

for i in x["result"]["heroes"]:
    y = y + str(i["id"]) + '->"' + i["name"] + '",'

for i in a["result"]["items"]:
    b = b + str(i["id"]) + '->"' + i["localized_name"] + '",'
