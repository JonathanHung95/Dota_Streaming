package dota_streaming.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StructType, LongType, BooleanType, StructField}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.typedLit
import org.apache.spark.sql.Column

object StreamingJob extends App {
    // spark session

    val spark = SparkSession.builder
                            .appName("dota_streaming_app")
                            .master("local[*]")
                            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                            .getOrCreate()

    val currentDirectory = new java.io.File(".").getCanonicalPath

    // json schema
    
    val jsonSchema = StructType(Array(
        StructField("account_id", LongType, true),
        StructField("backpack_0", LongType, true),
        StructField("backpack_1", LongType, true),
        StructField("backpack_2", LongType, true),
        StructField("cluster", LongType, true),
        StructField("duration", LongType, true),
        StructField("gold_per_min", LongType, true),
        StructField("hero_id", LongType, true),
        StructField("item_0", LongType, true),
        StructField("item_1", LongType, true),
        StructField("item_2", LongType, true),
        StructField("item_3", LongType, true),
        StructField("item_4", LongType, true),
        StructField("item_5", LongType, true),
        StructField("item_neutral", LongType, true),
        StructField("leaver_status", LongType, true),
        StructField("lobby_type", LongType, true),
        StructField("match_id", LongType, true),
        StructField("player_slot", LongType, true),
        StructField("radiant_win", LongType, true),
        //StructField("start_time", LongType, true),
        StructField("xp_per_min", LongType, true)
    ))

    val kafkaReaderConfig = KafkaReaderConfig("kafka:9092", "dbserver1.dota.match_data")
    new StreamingJobExecutor(spark, kafkaReaderConfig, currentDirectory + "/checkpoint/job", jsonSchema).execute()
}

case class KafkaReaderConfig(kafkaBootstrapServers: String, topics: String, startingOffsets: String = "latest")

class StreamingJobExecutor(spark: SparkSession, kafkaReaderConfig: KafkaReaderConfig, checkpointLocation: String, schema: StructType) {

    import spark.implicits._

    def read(): DataFrame = {
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", kafkaReaderConfig.kafkaBootstrapServers)
            .option("subscribe", kafkaReaderConfig.topics)
            .option("startingOffsets", kafkaReaderConfig.startingOffsets)
            .load()
    }

    def execute() = {
        // get kafka data -> json

        val df = read().select(from_json($"value".cast("string"), schema).as("value"))

        
                df.writeStream.option("checkpointLocation", "/checkpoint/job")
                 .format("console")
                 .option("truncate", "false")
                 .start()
                 .awaitTermination() 

        // convert unix time to timestamp

        //val df2 = df.withColumn("start_time", from_unixtime(col("start_time")))

        // map of heroes and items (id -> name)
        /*
        val heroMap: Column = typedLit(Map(1->"antimage",2->"axe",3->"bane",4->"bloodseeker",5->"crystal_maiden",6->"drow_ranger",7->"earthshaker",8->"juggernaut",9->"mirana",11->"nevermore",10->"morphling",12->"phantom_lancer",13->"puck",14->"pudge",15->"razor",16->"sand_king",17->"storm_spirit",18->"sven",19->"tiny",20->"vengefulspirit",21->"windrunner",22->"zuus",23->"kunkka",25->"lina",31->"lich",26->"lion",27->"shadow_shaman",28->"slardar",29->"tidehunter",30->"witch_doctor",32->"riki",33->"enigma",34->"tinker",35->"sniper",36->"necrolyte",37->"warlock",38->"beastmaster",39->"queenofpain",40->"venomancer",41->"faceless_void",42->"skeleton_king",43->"death_prophet",44->"phantom_assassin",45->"pugna",46->"templar_assassin",47->"viper",48->"luna",49->"dragon_knight",50->"dazzle",51->"rattletrap",52->"leshrac",53->"furion",54->"life_stealer",55->"dark_seer",56->"clinkz",57->"omniknight",58->"enchantress",59->"huskar",60->"night_stalker",61->"broodmother",62->"bounty_hunter",63->"weaver",64->"jakiro",65->"batrider",66->"chen",67->"spectre",69->"doom_bringer",68->"ancient_apparition",70->"ursa",71->"spirit_breaker",72->"gyrocopter",73->"alchemist",74->"invoker",75->"silencer",76->"obsidian_destroyer",77->"lycan",78->"brewmaster",79->"shadow_demon",80->"lone_druid",81->"chaos_knight",82->"meepo",83->"treant",84->"ogre_magi",85->"undying",86->"rubick",87->"disruptor",88->"nyx_assassin",89->"naga_siren",90->"keeper_of_the_light",91->"wisp",92->"visage",93->"slark",94->"medusa",95->"troll_warlord",96->"centaur",97->"magnataur",98->"shredder",99->"bristleback",100->"tusk",101->"skywrath_mage",102->"abaddon",103->"elder_titan",104->"legion_commander",106->"ember_spirit",107->"earth_spirit",109->"terrorblade",110->"phoenix",111->"oracle",105->"techies",112->"winter_wyvern",113->"arc_warden",108->"abyssal_underlord",114->"monkey_king",120->"pangolier",119->"dark_willow",121->"grimstroke",129->"mars",126->"void_spirit",128->"snapfire",123->"hoodwink",135->"dawnbreaker"))
        val itemMap: Column = typedLit(Map(1->"Blink Dagger",600->"Overwhelming Blink",603->"Swift Blink",604->"Arcane Blink",606->"Recipe: Arcane Blink",607->"Recipe: Swift Blink",608->"Recipe: Overwhelming Blink",2->"Blades of Attack",3->"Broadsword",4->"Chainmail",5->"Claymore",6->"Helm of Iron Will",7->"Javelin",8->"Mithril Hammer",9->"Platemail",10->"Quarterstaff",11->"Quelling Blade",237->"Faerie Fire",265->"Infused Raindrops",244->"Wind Lace",12->"Ring of Protection",182->"Stout Shield",246->"Recipe: Moon Shard",247->"Moon Shard",13->"Gauntlets of Strength",14->"Slippers of Agility",15->"Mantle of Intelligence",16->"Iron Branch",17->"Belt of Strength",18->"Band of Elvenskin",19->"Robe of the Magi",20->"Circlet",261->"Crown",21->"Ogre Axe",22->"Blade of Alacrity",23->"Staff of Wizardry",24->"Ultimate Orb",25->"Gloves of Haste",485->"Blitz Knuckles",26->"Morbid Mask",473->"Voodoo Mask",27->"Ring of Regen",279->"Ring of Tarrasque",28->"Sage's Mask",29->"Boots of Speed",30->"Gem of True Sight",31->"Cloak",32->"Talisman of Evasion",33->"Cheese",34->"Magic Stick",35->"Recipe: Magic Wand",36->"Magic Wand",37->"Ghost Scepter",38->"Clarity",216->"Enchanted Mango",39->"Healing Salve",40->"Dust of Appearance",41->"Bottle",42->"Observer Ward",43->"Sentry Ward",217->"Recipe: Observer and Sentry Wards",218->"Observer and Sentry Wards",44->"Tango",241->"Tango (Shared)",45->"Animal Courier",286->"Flying Courier",46->"Town Portal Scroll",47->"Recipe: Boots of Travel",219->"Recipe: Boots of Travel 2",48->"Boots of Travel",220->"Boots of Travel 2",49->"Recipe: Phase Boots",50->"Phase Boots",51->"Demon Edge",52->"Eaglesong",53->"Reaver",54->"Sacred Relic",55->"Hyperstone",56->"Ring of Health",57->"Void Stone",58->"Mystic Staff",59->"Energy Booster",60->"Point Booster",61->"Vitality Booster",593->"Fluffy Hat",62->"Recipe: Power Treads",63->"Power Treads",653->"Recipe: Grandmaster's Glaive",655->"Grandmaster's Glaive",64->"Recipe: Hand of Midas",65->"Hand of Midas",66->"Recipe: Oblivion Staff",67->"Oblivion Staff",533->"Recipe: Witch Blade",534->"Witch Blade",68->"Recipe: Perseverance",69->"Perseverance",70->"Recipe: Poor Man's Shield",71->"Poor Man's Shield",731->"Satchel",72->"Recipe: Bracer",73->"Bracer",74->"Recipe: Wraith Band",75->"Wraith Band",76->"Recipe: Null Talisman",77->"Null Talisman",78->"Recipe: Mekansm",79->"Mekansm",80->"Recipe: Vladmir's Offering",81->"Vladmir's Offering",85->"Recipe: Buckler",86->"Buckler",87->"Recipe: Ring of Basilius",88->"Ring of Basilius",268->"Recipe: Holy Locket",269->"Holy Locket",89->"Recipe: Pipe of Insight",90->"Pipe of Insight",91->"Recipe: Urn of Shadows",92->"Urn of Shadows",93->"Recipe: Headdress",94->"Headdress",95->"Recipe: Scythe of Vyse",96->"Scythe of Vyse",97->"Recipe: Orchid Malevolence",98->"Orchid Malevolence",245->"Recipe: Bloodthorn",250->"Bloodthorn",251->"Recipe: Echo Sabre",252->"Echo Sabre",99->"Recipe: Eul's Scepter of Divinity",100->"Eul's Scepter of Divinity",612->"Recipe: Wind Waker",610->"Wind Waker",233->"Recipe: Aether Lens",232->"Aether Lens",101->"Recipe: Force Staff",102->"Force Staff",262->"Recipe: Hurricane Pike",263->"Hurricane Pike",103->"Recipe: Dagon",197->"Recipe: Dagon",198->"Recipe: Dagon",199->"Recipe: Dagon",200->"Recipe: Dagon",104->"Dagon",201->"Dagon",202->"Dagon",203->"Dagon",204->"Dagon",105->"Recipe: Necronomicon",191->"Recipe: Necronomicon",192->"Recipe: Necronomicon",106->"Necronomicon",193->"Necronomicon",194->"Necronomicon",107->"Recipe: Aghanim's Scepter",108->"Aghanim's Scepter",270->"Recipe: Aghanim's Blessing",271->"Aghanim's Blessing",727->"Aghanim's Blessing - Roshan",609->"Aghanim's Shard",725->"Aghanim's Shard - Roshan",109->"Recipe: Refresher Orb",110->"Refresher Orb",111->"Recipe: Assault Cuirass",112->"Assault Cuirass",113->"Recipe: Heart of Tarrasque",114->"Heart of Tarrasque",115->"Recipe: Black King Bar",116->"Black King Bar",117->"Aegis of the Immortal",118->"Recipe: Shiva's Guard",119->"Shiva's Guard",120->"Recipe: Bloodstone",121->"Bloodstone",122->"Recipe: Linken's Sphere",123->"Linken's Sphere",221->"Recipe: Lotus Orb",226->"Lotus Orb",222->"Recipe: Meteor Hammer",223->"Meteor Hammer",224->"Recipe: Nullifier",225->"Nullifier",255->"Recipe: Aeon Disk",256->"Aeon Disk",258->"Recipe: Kaya",259->"Kaya",369->"Trident",276->"DOTA_Tooltip_Ability_combo_breaker",260->"Refresher Shard",266->"Recipe: Spirit Vessel",267->"Spirit Vessel",124->"Recipe: Vanguard",125->"Vanguard",243->"Recipe: Crimson Guard",242->"Crimson Guard",126->"Recipe: Blade Mail",127->"Blade Mail",128->"Recipe: Soul Booster",129->"Soul Booster",130->"Recipe: Hood of Defiance",131->"Hood of Defiance",691->"Recipe: Eternal Shroud",692->"Eternal Shroud",132->"Recipe: Divine Rapier",133->"Divine Rapier",134->"Recipe: Monkey King Bar",135->"Monkey King Bar",136->"Recipe: Radiance",137->"Radiance",138->"Recipe: Butterfly",139->"Butterfly",140->"Recipe: Daedalus",141->"Daedalus",142->"Recipe: Skull Basher",143->"Skull Basher",144->"Recipe: Battle Fury",145->"Battle Fury",146->"Recipe: Manta Style",147->"Manta Style",148->"Recipe: Crystalys",149->"Crystalys",234->"Recipe: Dragon Lance",236->"Dragon Lance",150->"Recipe: Armlet of Mordiggian",151->"Armlet of Mordiggian",183->"Recipe: Shadow Blade",152->"Shadow Blade",248->"Recipe: Silver Edge",249->"Silver Edge",153->"Recipe: Sange and Yasha",154->"Sange and Yasha",272->"Recipe: Kaya and Sange",273->"Kaya and Sange",274->"Recipe: Yasha and Kaya",277->"Yasha and Kaya",155->"Recipe: Satanic",156->"Satanic",157->"Recipe: Mjollnir",158->"Mjollnir",159->"Recipe: Eye of Skadi",160->"Eye of Skadi",161->"Recipe: Sange",162->"Sange",163->"Recipe: Helm of the Dominator",164->"Helm of the Dominator",633->"Recipe: Helm of the Overlord",635->"Helm of the Overlord",165->"Recipe: Maelstrom",166->"Maelstrom",1565->"Recipe: Gleipnir",1466->"Gleipnir",167->"Recipe: Desolator",168->"Desolator",169->"Recipe: Yasha",170->"Yasha",171->"Recipe: Mask of Madness",172->"Mask of Madness",173->"Recipe: Diffusal Blade",174->"Diffusal Blade",175->"Recipe: Ethereal Blade",176->"Ethereal Blade",177->"Recipe: Soul Ring",178->"Soul Ring",179->"Recipe: Arcane Boots",180->"Arcane Boots",228->"Recipe: Octarine Core",235->"Octarine Core",181->"Orb of Venom",240->"Blight Stone",640->"Recipe: Orb of Corrosion",569->"Orb of Corrosion",599->"Recipe: Falcon Blade",596->"Falcon Blade",597->"Recipe: Mage Slayer",598->"Mage Slayer",184->"Recipe: Drum of Endurance",185->"Drum of Endurance",186->"Recipe: Medallion of Courage",187->"Medallion of Courage",227->"Recipe: Solar Crest",229->"Solar Crest",188->"Smoke of Deceit",257->"Tome of Knowledge",189->"Recipe: Veil of Discord",190->"Veil of Discord",230->"Recipe: Guardian Greaves",231->"Guardian Greaves",205->"Recipe: Rod of Atos",206->"Rod of Atos",238->"Recipe: Iron Talon",239->"Iron Talon",207->"Recipe: Abyssal Blade",208->"Abyssal Blade",209->"Recipe: Heaven's Halberd",210->"Heaven's Halberd",211->"Recipe: Ring of Aquila",212->"Ring of Aquila",213->"Recipe: Tranquil Boots",214->"Tranquil Boots",215->"Shadow Amulet",253->"Recipe: Glimmer Cape",254->"Glimmer Cape",1021->"River Vial: Chrome",1022->"River Vial: Dry",1023->"River Vial: Slime",1024->"River Vial: Oil",1025->"River Vial: Electrified",1026->"River Vial: Potion",1027->"River Vial: Blood",1028->"Tombstone",1029->"Super Blink Dagger",1030->"Pocket Tower",1032->"Pocket Roshan",287->"Keen Optic",288->"Grove Bow",289->"Quickening Charm",290->"Philosopher's Stone",291->"Force Boots",292->"Stygian Desolator",293->"Phoenix Ash",294->"Seer Stone",295->"Greater Mango",302->"Elixir",297->"Vampire Fangs",298->"Craggy Coat",299->"Greater Faerie Fire",300->"Timeless Relic",301->"Mirror Shield",303->"Recipe: Ironwood Tree",304->"Ironwood Tree",328->"Mango Tree",305->"Royal Jelly",306->"Pupil's Gift",307->"Tome of Aghanim",308->"Repair Kit",309->"Mind Breaker",310->"Third Eye",311->"Spell Prism",325->"Prince's Knife",330->"Witless Shako",334->"Imp Claw",335->"Flicker",336->"Telescope",326->"Spider Legs",327->"Helm of the Undying",329->"Recipe: Vambrace",331->"Vambrace",312->"Horizon",313->"Fusion Rune",354->"Ocean Heart",355->"Broom Handle",356->"Trusty Shovel",357->"Nether Shawl",358->"Dragon Scale",359->"Essence Ring",360->"Clumsy Net",361->"Enchanted Quiver",362->"Ninja Gear",363->"Illusionist's Cape",364->"Havoc Hammer",365->"Magic Lamp",366->"Apex",367->"Ballista",368->"Woodland Striders",275->"Recipe: Trident",370->"Book of the Dead",317->"Recipe: Fallen Sky",371->"Fallen Sky",372->"Pirate Hat",373->"Dimensional Doorway",374->"Ex Machina",375->"Faded Broach",376->"Paladin Sword",377->"Minotaur Horn",378->"Orb of Destruction",379->"The Leveller",349->"Arcane Ring",381->"Titan Sliver",565->"Chipped Vest",566->"Wizard Glass",570->"Gloves of Travel",573->"Elven Tunic",574->"Cloak of Flames",575->"Venom Gland",571->"Trickster Cloak",576->"Helm of the Gladiator",577->"Possessed Mask",578->"Ancient Perseverance",637->"Star Mace",638->"Penta-Edged Sword",582->"Oakheart",674->"Warhammer",680->"Bullwhip",675->"Psychic Headband",676->"Ceremonial Robe",686->"Quicksilver Amulet",677->"Book of Shadows",678->"Giant's Ring",679->"Shadow of Vengeance",585->"Stormcrafter",588->"Overflowing Elixir",589->"Fairy's Trinket"))

        val df3 = df.withColumn("hero_id", heroMap(col("hero_id"))).withColumnRenamed("hero_id", "hero")
                    .withColumn("item_0", itemMap(col("item_0")).alias("item_0"))
                    .withColumn("item_1", itemMap(col("item_1")).alias("item_1"))
                    .withColumn("item_2", itemMap(col("item_2")).alias("item_2"))
                    .withColumn("item_3", itemMap(col("item_3")).alias("item_3"))
                    .withColumn("item_4", itemMap(col("item_4")).alias("item_4"))
                    .withColumn("item_5", itemMap(col("item_5")).alias("item_5"))
                    .withColumn("backpack_0", itemMap(col("backpack_0")).alias("backpack_0"))
                    .withColumn("backpack_1", itemMap(col("backpack_1")).alias("backpack_1"))
                    .withColumn("backpack_2", itemMap(col("backpack_2")).alias("backpack_2"))
                    .withColumn("item_neutral", itemMap(col("item_neutral")).alias("item_neutral"))

        // convert radiant_win to win (based on whether the player won the match or not)

        val df4 = df3.withColumn("win", when((col("player_slot") > 10) && (col("radiant_win") === true), false)
                                        .when((col("player_slot") < 10) && (col("radiant_win") === true), true)
                                        .when((col("player_slot") > 10) && (col("radiant_win") != true), true)
                                        .when((col("player_slot") < 10) && (col("radiant_win") != true), false))

        // convert all the individual item columns into a single array column: items
        /*
        val df5 = df4.withColumn("items", array(col("item_0"), col("item_1"), col("item_2"), col("item_3"), col("item_4"), col("item_5"), col("backpack_0"), 
                                                col("backpack_1"), col("backpack_2"), col("item_neutral")))
                        .drop("item_0", "item_1", "item_2", "item_3", "item_4", "item_5", "backpack_0", "backpack_1", "backpack_2", "item_neutral")
        */

        df4.select($"value.payload.after.*")
            .writeStream
            .queryName("write_to_hudi")
            .foreachBatch{
            (batchDF: DataFrame, _: Long) => {
                batchDF.write.format("org.apache.hudi")
                .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
                .option("hoodie.datasource.write.precombine.field", "duration")
                .option("hoodie.datasource.write.recordkey.field", "hero")
                .option("hoodie.datasource.write.partitionpath.field", "match_id")
                .option("hoodie.table.name", "match_data")
                .option("hoodie.datasource.write.hive_style_partitioning", true)
                .mode(SaveMode.Append)
                .save("/tmp/sparkHudi/match_data")
                }
            }
            .option("checkpointLocation", "/tmp/sparkHudi/checkpoint/")
            .start()
            .awaitTermination()
    */
    }
}