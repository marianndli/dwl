import json

def lambda_handler(event, context):
    
    import requests
    import json
    import datetime
    import boto3
    
    try:
        velospot_stations = [
        "Station%20FFS%20-%20Muralto",
        "Posteggio%20FART%20S.%20Antonio%20-%20Locarno",
        "Piazza%20Solduno%20-%20Locarno",
        "Solduno%20S.%20Martino%20-%20Locarno",
        "Ponte%20Brolla%20-%20Locarno",
        "Stazione%20FFS%20Riazzino%20-%20Locarno",
        "Intragna%20-%20Centovalli",
        "Cavigliano%20-%20Stazione%20-%20Terre%20di%20Pedemonte",
        "Tegna%20-%20Stazione%20-%20Terre%20di%20Pedemonte",
        "Porticciolo%20Rivapiana%20-%20Minusio",
        "Stazione%20FFS%20-%20Tenero",
        "Stazione%20SBB%20-%20Gordola",
        "Stazione%20SBB%20-%20Cadenazzo",
        "Quartino%20-%20Rivamonte%20-%20Gambarogno",
        "Stazione%20FFS%20-%20Giubiasco",
        "Bahnhof%20Chaux-de-Fonds%20-%20CdF",
        "Parking%20-%20Rue%20de%20la%20Surfrète%20-%20Martigny",
        "Bahnhof%20Martigny%20-%20Martigny",
        "Rue%20des%20Prés-Aubert%20-%20Martigny",
        "Funiculaire%20-%20Vevey",
        "Bar%20Buffet%20Express%20-%20SBB%20Bahnhof%20-%20Vevey",
        "A%20l'imprévu%20-%20Gare%20CFF%20-%20TdP",
        "Restaurant%20le%20Petit%20Baigneur%20-%20Piscine%20de%20La%20Maladaire/Parking%20-%20Montreux",
        "1820%20Restaurant%20&%20Cocktails%20Bar%20-%20Montreux",
        "Robert-Walser%20Platz%20-%20Biel/Bienne",
        "Bahnhof%20Biel%20-%20Biel/Bienne",
        "Rotonde%20-%20Biel/Bienne",
        "Grenchenstrasse%20-%20Biel/Bienne",
        "Bahnhof%20Boujean%20-%20Biel/Bienne",
        "Stazione%20FFS%20-%20Bellinzona",
        "FFS%20-%20Sant%20Antonino",
        "Gare%20CFF%20-%20Villeneuve",
        "Zeugma%20-%20Pizza%20au%20feu%20de%20bois%20et%20Menu%20du%20jour%20-%20021%20546%2021%2021%20-%20Rue%20des%20Moulins%20-%20Vevey",
        "Bahnhof%20Siggenthal%20Würenlingen%20-%20PSI",
        "Hôtel%20de%20Ville%20-%20Place%20du%20Marché%20-%20Aigle",
        "Café%20des%20Petits%20Trains%20-%20Pizzeria%20&%20Snacks%20-%20Place%20de%20la%20Gare%203%20-%20Aigle",
        "Cinéma%20Cosmopolis%20-%20Aigle",
        "Rue%20du%20Collège%20-%20Aigle",
        "Außer%20Betrieb%20wegen%20Bauarbeiten%20-%20Sharing%20Zone%20SBB%20-%20Parkplatz%20-%20Basel",
        "GAIA%20–%20www.gaiahotel.ch%20-%20Basel",
        "Nestlé%20Nespresso%20-%20Vevey",
        "Rosentalstrasse%2056%20-%20Basel",
        "Heumattstrasse%20-%20Basel",
        "Moi%20pour%20Toit%20-%20Martigny",
        "Pizzeria%20Restaurant%20d'Octodure%20-%20Martigny",
        "Sharing%20Zone%20SBB%20-%20Nord%20-%20Basel",
        "Sharing%20Zone%20SBB%20-%20Süd%20-%20Basel",
        "Velostation%202221%20-%20P+R%20Terminal%20Car%20-%20Martigny",
        "Außer%20Betrieb%20-%20Schwarzwaldallee%20185%20-%20Basel",
        "St.%20Jakobs-Strasse%20397%20-%20Basel",
        "Gare%20de%20Ardon%20-%20Agglo%20Valais%20central",
        "Gare%20de%20Châteauneuf-Conthey%20-%20Agglo%20Valais%20central",
        "Salgesch%20Bahnhof%20-%20Agglo%20Valais%20central",
        "Station%20FFS%20-%20Muralto",
        "Posteggio%20FART%20S.%20Antonio%20-%20Locarno",
        "Piazza%20Solduno%20-%20Locarno",
        "Solduno%20S.%20Martino%20-%20Locarno",
        "Ponte%20Brolla%20-%20Locarno",
        "Stazione%20FFS%20Riazzino%20-%20Locarno",
        "Intragna%20-%20Centovalli",
        "Cavigliano%20-%20Stazione%20-%20Terre%20di%20Pedemonte",
        "Tegna%20-%20Stazione%20-%20Terre%20di%20Pedemonte",
        "Porticciolo%20Rivapiana%20-%20Minusio",
        "Stazione%20FFS%20-%20Tenero",
        "Stazione%20SBB%20-%20Gordola",
        "Stazione%20SBB%20-%20Cadenazzo",
        "Quartino%20-%20Rivamonte%20-%20Gambarogno",
        "Stazione%20FFS%20-%20Giubiasco",
        "Bahnhof%20Chaux-de-Fonds%20-%20CdF",
        "Parking%20-%20Rue%20de%20la%20Surfrète%20-%20Martigny",
        "Bahnhof%20Martigny%20-%20Martigny",
        "Rue%20des%20Prés-Aubert%20-%20Martigny",
        "Funiculaire%20-%20Vevey",
        "Bar%20Buffet%20Express%20-%20SBB%20Bahnhof%20-%20Vevey",
        "A%20l'imprévu%20-%20Gare%20CFF%20-%20TdP",
        "Restaurant%20le%20Petit%20Baigneur%20-%20Piscine%20de%20La%20Maladaire/Parking%20-%20Montreux",
        "1820%20Restaurant%20&%20Cocktails%20Bar%20-%20Montreux",
        "Robert-Walser%20Platz%20-%20Biel/Bienne",
        "Bahnhof%20Biel%20-%20Biel/Bienne",
        "Rotonde%20-%20Biel/Bienne",
        "Grenchenstrasse%20-%20Biel/Bienne",
        "Bahnhof%20Boujean%20-%20Biel/Bienne",
        "Stazione%20FFS%20-%20Bellinzona",
        "FFS%20-%20Sant%20Antonino",
        "Außer%20Betrieb%20wegen%20Bauarbeiten%20-%20Sharing%20Zone%20SBB%20-%20Parkplatz%20-%20Basel",
        "GAIA%20–%20www.gaiahotel.ch%20-%20Basel",
        "Nestlé%20Nespresso%20-%20Vevey",
        "Rosentalstrasse%2056%20-%20Basel",
        "Außer%20Betrieb%20-%20Schwarzwaldallee%20185%20-%20Basel",
        "St.%20Jakobs-Strasse%20397%20-%20Basel",
        "Gare%20de%20Châteauneuf-Conthey%20-%20Agglo%20Valais%20central",
        "Salgesch%20Bahnhof%20-%20Agglo%20Valais%20central",
        "Badischer%20Bahnhof%20-%20Schwarzwaldallee%20234%20-%20Basel",
        "Voltastrasse%20104%20-%20Basel",
        "Außer%20Betrieb%20wegen%20Bauarbeiten%20-%20Badischer%20Bahnhof%20Nord%20-%20Schwarzwaldallee%20200%20-%20Basel",
        "Badischer%20Bahnhof%20Süd%20-%20Schwarzwaldallee%20184%20-%20Basel"
        ]
        
        
        # API URL for the data
        api_url_part1 = "https://api.sharedmobility.ch/v1/sharedmobility/find?searchText="
        api_url_part2 = "&searchField=ch.bfe.sharedmobility.station.name&offset=0&geometryFormat=esrijson"
        
        current_time = datetime.datetime.now()
        formatted_time = current_time.strftime("%Y-%m-%d %H:%M:%S")
               
        data = []
            
        for station in velospot_stations:
            act_url=api_url_part1+station+api_url_part2
            response_new = requests.get(act_url)
            data_new = response_new.json()
            for item in data_new:
                item['timestamp'] = formatted_time
            data.extend(data_new)
        
        
        # Initialize S3 client
        s3 = boto3.client('s3')
    
        # Save JSON data to S3 bucket
        bucket_name = 'velospotbucket'
        key = formatted_time+'-velospot.json'
        s3.put_object(Bucket=bucket_name, Key=key, Body=json.dumps(data))
    
        return {
            'statusCode': 200,
            'body': json.dumps('JSON data saved to S3')
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')}
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
