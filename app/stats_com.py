from app.db import get_db
import json



def count_nodes(db,id):
    query = f"""
    MATCH(n:NFT)
    WHERE n.boredapeyachtclub_com_id_list[0] = {id}
    RETURN count(n) as count
    """
    result =db.run_query('neo4j',query)
    return (result[0]['count'])

def testing_stats():
    db= get_db()
    query = """
    MATCH(n:Community_Info)
    RETURN n
    
    """
    result =db.run_query('neo4j',query)
    p = result[0]['n']._properties
    complete = json.loads(p['complete_id_list']) 
    deg= json.loads(p['degods_id_list'])
    bored=  json.loads(p['boredapeyachtclub_id_list'])
    counter=[]
    for i in bored[0]:
        counter.append(count_nodes(db,i))

        
    print(counter)
    print(sum(counter))

