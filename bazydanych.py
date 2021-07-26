
def fun():	
	import json
	import psycopg2	
	import sys
	conn = psycopg2.connect("dbname=postgres user=postgres password=qwerty")
	cur = conn.cursor()
	
	
	
	if len(sys.argv) >1:
		if sys.argv[1]=="--init":
			cur.execute("select * from information_schema.tables where table_name='flights';")
			records = cur.fetchall()
			if len(records)>0:
			 	cur.execute("DROP TABLE flights;")
			cur.execute("CREATE TABLE flights(id INT,fromwhere varchar(4),towhere varchar(4),takeoff_time TIMESTAMP,landing_time TIMESTAMP);")
			print('{"status": "OK"}')
	else:
		
		for line in sys.stdin:	
			cos =json.loads(line)
			if cos["function"] == 'flight':
				 para=cos["params"]
				 airs=para["airports"]
				 ile=len(airs)-1
				 i=0
				 while i<ile:
				 	a=airs[i]
				 	b=airs[i+1]
				 	aa=a["airport"]
				 	bb=b["airport"]
				 	cc=a[ "takeoff_time" ]
				 	dd=b[ "landing_time" ]
				 	cur.execute("INSERT INTO flights(id,fromwhere,towhere,takeoff_time,landing_time) VALUES (%s, %s,%s,%s,%s);",(para["id"],aa,bb,cc,dd))		
				 	i=i+1
				 print('{"status": "OK"}')
			elif cos["function"] == 'list_flights':
				print (cos["params"])
				para=cos["params"]
				xf=str(para["id"])
				cur.execute("WITH AA AS( SELECT  a.longitude a1, a.latitude a2, b.longitude b1, b.latitude b2 , id rid, fromwhere froms, towhere tos ,takeoff_time FROM flights JOIN airport a ON(a.iatacode=flights.fromwhere) JOIN airport b ON(b.iatacode=flights.towhere)),BB AS ( SELECT  a.longitude aa1, a.latitude aa2, b.longitude bb1, b.latitude bb2 FROM flights JOIN airport a ON(a.iatacode=flights.fromwhere) JOIN airport b ON(b.iatacode=flights.towhere) WHERE id= %s ) SELECT 'LINESTRING('|| a1 || ' ' || a2 ||', '|| b1 || ' ' || b2 || ')' ,  'LINESTRING('|| aa1 || ' ' || aa2 ||', '|| bb1 || ' ' || bb2 ||')' , rid , froms ,tos , takeoff_time FROM AA JOIN BB ON(1=1);" % (xf) )
				ala=cur.fetchone()
			
				x = {"status": "OK","data": list(ala)}
				
				print(x)
	
	conn.commit()
	cur.close()
	conn.close()
	
fun()
'''
SELECT 'LINESTRING('|| a.longitude || ' ' || a.latitude ||', '|| airport.longitude || ' ' || airport.latitude')' ,*
 FROM flights JOIN airport a ON(a.iatacode=flights.fromwhere) JOIN airport ON(airport.iatacode=flights.fromwhere);


WITH AA AS( SELECT  a.longitude a1, a.latitude a2, b.longitude b1, b.latitude b2 , id rid, fromwhere froms, towhere tos ,takeoff_time
 	     FROM flights JOIN airport a ON(a.iatacode=flights.fromwhere) JOIN airport b ON(b.iatacode=flights.towhere)
 	  ),
     BB AS ( SELECT  a.longitude aa1, a.latitude aa2, b.longitude bb1, b.latitude bb2 
 FROM flights JOIN airport a ON(a.iatacode=flights.fromwhere) JOIN airport b ON(b.iatacode=flights.towhere) WHERE id=12346)
 SELECT 'LINESTRING('|| a1 || ' ' || a2 ||', '|| b1 || ' ' || b2 || ')' ,  'LINESTRING('|| aa1 || ' ' || aa2 ||', '|| bb1 || ' ' || bb2 ||')' , rid , froms ,tos , takeoff_time
  FROM AA JOIN BB ON(1=1);


SELECT ST_Distance('LINESTRING(16.89 51.1, 18.47 54.38)'::geography, 'LINESTRING(19.1 50.5, 19.8 50.08)'::geography)/1000 as distance;

 AA.id, AA.fromwhere, AA.towhere,AA.takeoff_time

'LINESTRING('|| AA.a1 || ' ' || AA.a2 ||', '|| AA.b1 || ' ' || AA.b2')' ,  'LINESTRING('|| BB.a1 || ' ' || BB.a2 ||', '|| BB.b1 || ' ' || BB.b2')' 
'''
