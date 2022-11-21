import json
import boto3
from datetime import datetime
import datetime
import os
region = os.environ.get('AWS_REGION')




def lambda_handler(event, context):
    session = boto3.Session(aws_access_key_id=os.environ('global_aws_access_key_id'),
                        aws_secret_access_key=os.environ('global_aws_secret_access_key')

    adx = session.client(
        'dataexchange',
        region_name='us-east-1'
    )

    path = event['path']
    httpMethod = event['httpMethod']

    
    ######## ##     ## ##    ##  ######  ######## ####  #######  ##    ##  ######  
    ##       ##     ## ###   ## ##    ##    ##     ##  ##     ## ###   ## ##    ## 
    ##       ##     ## ####  ## ##          ##     ##  ##     ## ####  ## ##       
    ######   ##     ## ## ## ## ##          ##     ##  ##     ## ## ## ##  ######  
    ##       ##     ## ##  #### ##          ##     ##  ##     ## ##  ####       ## 
    ##       ##     ## ##   ### ##    ##    ##     ##  ##     ## ##   ### ##    ## 
    ##        #######  ##    ##  ######     ##    ####  #######  ##    ##  ######  
        
    def get_IMDB_api_response(querystring={}):
        return adx.send_api_asset(
            DataSetId="4b1f47d86b35356cf8fb6f15cc758c0e",
            RevisionId="4915c8e5e666a284124fc532ca8fbbe2",
            AssetId="f05f6f7ca415c8be7341f95bf1db34c5",
            RequestHeaders={"x-api-key": ""},
            Method="POST",
            Path="/v1",
            Body=querystring
        )
    
    
    def get_catalog(IMBD_nn):
        theRequest = "{\"query\": \
             \"{ \
                names(ids: \\\"" + IMBD_nn + "\\\") { \
                  nameText { \
                     text \
                  } \
                  credits(first:500) { \
                    edges { \
                        node { \
                            title { \
                                titleText { \
                                text \
                                } \
                                id \
                            } \
                        } \
                    } \
                  } \
                } \
               }\"}"
        res = get_IMDB_api_response(querystring=theRequest)
        fullInfo = json.loads(res["Body"])
        talentName = fullInfo["data"]["names"][0]["nameText"]["text"]
        titleList = fullInfo["data"]["names"][0]["credits"]["edges"]
    
        titleCatalog = []
        titleIDCatalog = []
        for oneTitle in titleList:
            titleName = oneTitle["node"]["title"]["titleText"]["text"]
            titleID = oneTitle["node"]["title"]["id"]
            titleinfo = {"title": oneTitle["node"]["title"]["titleText"]
                         ["text"], "titleID": oneTitle["node"]["title"]["id"]}
            if titleinfo not in titleCatalog:
                titleCatalog.append(titleinfo)
        # print(titleIDCatalog)
        # print(titleCatalog)
    
        return talentName, titleCatalog
    
    
    def get_talent_list(key_talent, key_talent_name, titleList):
        # iterate the title list, if key_talent is producer, then get big category talent for that title
    
        checker = "{'node': {'name': {'nameText': {'text': '" + \
            key_talent_name + "'}, 'id': '" + key_talent + "'}}}"
    
        # print(checker)
        big_talent_list = []
        for oneTitle in titleList:
            # print(oneTitle["titleID"])
            bigpull = "{\"query\": \
             \"{ \
                title(id: \\\"" + oneTitle["titleID"] + "\\\") { \
                    titleText { \
                         text \
                      } \
                    PRODUCERS: credits(first: 50, filter: { categories: [\\\"producer\\\"] }) { \
                        edges { \
                            node { \
                                name { \
                                    nameText { \
                                        text \
                                    } \
                                    id \
                                } \
                            } \
                        } \
                    } \
                    DIRECTORS: credits(first: 50, filter: { categories: [\\\"director\\\"] }) { \
                        edges { \
                            node { \
                                name { \
                                    nameText { \
                                        text \
                                    } \
                                    id \
                                } \
                            } \
                        } \
                    } \
                    MUSIC: credits(first: 50, filter: { categories: [\\\"music_department\\\"] }) { \
                        edges { \
                            node { \
                                name { \
                                    nameText { \
                                        text \
                                    } \
                                    id \
                                } \
                            } \
                        } \
                    } \
                    COSTUMES: credits(first: 50, filter: { categories: [\\\"costume_department\\\"] }) { \
                        edges { \
                            node { \
                                name { \
                                    nameText { \
                                        text \
                                    } \
                                    id \
                                } \
                            } \
                        } \
                    } \
                    CASTING: credits(first: 50, filter: { categories: [\\\"casting_department\\\"] }) { \
                        edges { \
                            node { \
                                name { \
                                    nameText { \
                                        text \
                                    } \
                                    id \
                                } \
                            } \
                        } \
                    } \
                    ART: credits(first: 50, filter: { categories: [\\\"art_department\\\"] }) { \
                        edges { \
                            node { \
                                name { \
                                    nameText { \
                                        text \
                                    } \
                                    id \
                                } \
                            } \
                        } \
                    } \
                    SOUND: credits(first: 50, filter: { categories: [\\\"sound_department\\\"] }) { \
                        edges { \
                            node { \
                                name { \
                                    nameText { \
                                        text \
                                    } \
                                    id \
                                } \
                            } \
                        } \
                    } \
                } \
            }\"}"
            res = get_IMDB_api_response(querystring=bigpull)
            fullTalent = json.loads(res["Body"])
            # print(fullTalent)
    
            producerlist = fullTalent["data"]["title"]["PRODUCERS"]["edges"]
            directorlist = fullTalent["data"]["title"]["DIRECTORS"]["edges"]
            musiclist = fullTalent["data"]["title"]["MUSIC"]["edges"]
            soundlist = fullTalent["data"]["title"]["SOUND"]["edges"]
            costumelist = fullTalent["data"]["title"]["COSTUMES"]["edges"]
            artlist = fullTalent["data"]["title"]["ART"]["edges"]
            castinglist = fullTalent["data"]["title"]["CASTING"]["edges"]
    
            # I couldn't get this to work -- it seemed visually a match, but didn't trigger
            # if checker in producerlist:
            #    print("Include this production")
    
            isProducer = False
            for oneproducer in producerlist:
                if oneproducer["node"]["name"]["id"] == key_talent:
                    isProducer = True
    
            if isProducer:
                for onetalent in producerlist:
                    addTalentDict = {"role": "Producer", "name": onetalent["node"]["name"]["nameText"]["text"],
                                     "talentID": onetalent["node"]["name"]["id"], "title": oneTitle["title"], "titleID": oneTitle["titleID"]}
                    # we don't need to add the key talent to the list
                    if onetalent["node"]["name"]["id"] != key_talent:
                        big_talent_list.append(addTalentDict)
    
                for onetalent in castinglist:
                    addTalentDict = {"role": "Casting", "name": onetalent["node"]["name"]["nameText"]["text"],
                                     "talentID": onetalent["node"]["name"]["id"], "title": oneTitle["title"], "titleID": oneTitle["titleID"]}
                    # we don't need to add the key talent to the list
                    if onetalent["node"]["name"]["id"] != key_talent:
                        big_talent_list.append(addTalentDict)
    
                for onetalent in directorlist:
                    addTalentDict = {"role": "Director", "name": onetalent["node"]["name"]["nameText"]["text"],
                                     "talentID": onetalent["node"]["name"]["id"], "title": oneTitle["title"], "titleID": oneTitle["titleID"]}
                    # we don't need to add the key talent to the list
                    if onetalent["node"]["name"]["id"] != key_talent:
                        big_talent_list.append(addTalentDict)
    
                for onetalent in musiclist:
                    addTalentDict = {"role": "Music", "name": onetalent["node"]["name"]["nameText"]["text"],
                                     "talentID": onetalent["node"]["name"]["id"], "title": oneTitle["title"], "titleID": oneTitle["titleID"]}
                    # we don't need to add the key talent to the list
                    if onetalent["node"]["name"]["id"] != key_talent:
                        big_talent_list.append(addTalentDict)
    
                for onetalent in soundlist:
                    addTalentDict = {"role": "Sound", "name": onetalent["node"]["name"]["nameText"]["text"],
                                     "talentID": onetalent["node"]["name"]["id"], "title": oneTitle["title"], "titleID": oneTitle["titleID"]}
                    # we don't need to add the key talent to the list
                    if onetalent["node"]["name"]["id"] != key_talent:
                        big_talent_list.append(addTalentDict)
    
                for onetalent in costumelist:
                    addTalentDict = {"role": "Costume", "name": onetalent["node"]["name"]["nameText"]["text"],
                                     "talentID": onetalent["node"]["name"]["id"], "title": oneTitle["title"], "titleID": oneTitle["titleID"]}
                    # we don't need to add the key talent to the list
                    if onetalent["node"]["name"]["id"] != key_talent:
                        big_talent_list.append(addTalentDict)
    
                for onetalent in artlist:
                    addTalentDict = {"role": "Art", "name": onetalent["node"]["name"]["nameText"]["text"],
                                     "talentID": onetalent["node"]["name"]["id"], "title": oneTitle["title"], "titleID": oneTitle["titleID"]}
                    # we don't need to add the key talent to the list
                    if onetalent["node"]["name"]["id"] != key_talent:
                        big_talent_list.append(addTalentDict)
    
        return big_talent_list
    
    
    def make_the_list(talentList, key_title=""):
        theSorted = sorted(talentList, key=lambda n: n['talentID'])
    
        # we will sort frequent work together to the top, then sort people who worked on key_title project
        one_talent = empty_talent = {
            'role': '', 'name': '', 'talentID': '', 'titles': '', 'awards': '', 'weight': '0', 'working': ''}
        weighted_list = []
    
        for one_row in theSorted:
            # we're going to create a weighted list, 5 points per work-together, 3 points if you worked on key_title, later will add 1 point for awards
    
            if one_row["talentID"] != one_talent["talentID"]:
                weighted_list.append(one_talent)
                one_talent = {'role': one_row["role"], 'name': one_row["name"],
                              'talentID': one_row["talentID"], 'titles': one_row["title"], 'awards': '', 'weight': '0', 'working': ''}
                if one_row["titleID"] == key_title:
                    one_talent["weight"] = str(int(one_talent["weight"]) + 3)
            else:
                # boost for multiple workings
                one_talent["weight"] = str(int(one_talent["weight"]) + 5)
                if one_row["titleID"] == key_title:
                    one_talent["weight"] = str(int(one_talent["weight"]) + 3)
                new_title = ", " + one_row["title"]
                one_talent["titles"] = one_talent["titles"] + new_title
    
        weighted_list.pop(0)  # this gets rid of empty first record
    
        weighted_sortlist = sorted(
            weighted_list, key=lambda n: n['weight'], reverse=True)
    
        return weighted_sortlist
    
    
    def are_they_working(talentList, list_size=5):
        sort1 = sorted(talentList, key=lambda n: n['weight'], reverse=True)
        working_list = sorted(talentList, key=lambda n: n['role'])
    
       
    
        theOrder = ["Producer", "Casting", "Director",
                    "Costume", "Art", "Music", "Sound"]
    
        final_list = []
    
        for oneRole in theOrder:
            keyValList = [oneRole]
            sublist = [d for d in talentList if d['role'] in keyValList]
            # final_list.append(sublist[0:list_size])
            final_list = final_list + sublist[0:list_size]
    
        return final_list
    
##     ##    ###    #### ##    ##     ######   #######  ########  ######## 
###   ###   ## ##    ##  ###   ##    ##    ## ##     ## ##     ## ##       
#### ####  ##   ##   ##  ####  ##    ##       ##     ## ##     ## ##       
## ### ## ##     ##  ##  ## ## ##    ##       ##     ## ##     ## ######   
##     ## #########  ##  ##  ####    ##       ##     ## ##     ## ##       
##     ## ##     ##  ##  ##   ###    ##    ## ##     ## ##     ## ##       
##     ## ##     ## #### ##    ##     ######   #######  ########  ######## 

    key_talent = event['queryStringParameters']['key_talent']
    key_title = event['queryStringParameters']['key_title']
    max_per_role = int(event['queryStringParameters']['max_per_role'])
    
        
    # STEP 1: get a list of every production key talent has been in
    key_talent_name, theCatalog = get_catalog(key_talent)
    #print(theCatalog)
    
    # STEP 2: pull the talent for each production, but only keep it if key talent was a director/producer
    theTalent = get_talent_list(key_talent, key_talent_name, theCatalog)
    
    
    # STEP 3: highlight multiples and most recent
    theList = make_the_list(theTalent, key_title)
    
    # STEP 4: add weight for AWARDS -- can't find this data
    
    # STEP 5: are they currently working? -- I couldn't separate out pre-production or ongoing series, e.g. if a show ended in 2019,
    #         then obviously they are not working on that, but if it has 2022 episodes, they may or may be committed
    theFinalList = are_they_working(theList, max_per_role)


    theResponse = {"key_talent":key_talent_name,"key_title":key_title,"team":theFinalList}
    
        
        
    
    return {
        'statusCode': 200,
        'body': json.dumps(theResponse)
    }
