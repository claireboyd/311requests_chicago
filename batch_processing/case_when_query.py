
text = """
| 311IOC         |
| AAD            |
| AVN            |
| BAM            |
| BBA            |
| BBC            |
| BBD            |
| BPI            |
| CAFE           |
| CIAC           |
| CORNVEND       |
| EAE            |
| EAF            |
| EAQ            |
| ESPC           |
| FAC            |
| FPC            |
| HFF            |
| PBD            |
| PBLDR          |
| PBS            |
| PCB            |
| PCC            |
| PET            |
| PHB            |
| PHF            |
| PSL            |
| QAC            |
| RFC            |
| SCB            |
| SCS            |
| SCT            |
| SCX            |
| SDO            |
| SDP            |
| SDW            |
| SEC            |
| SEL            |
| SFC            |
| SFQ            |
| SGA            |
| SGV            |
| SIE            |
| SRRC           |
| SRRP           |
| SWSNOREM       |
| TNP            |
| VBL            |
| WCA3           |
| WM3            |
| AAE            |
| AAF            |
| AAI            |
| BAG            |
| BBK            |
| BUNGALOW       |
| CHECKFOR       |
| CSC            |
| CSF            |
| CSP            |
| CST            |
| DBPC           |
| EAB            |
| EBD            |
| GRAF           |
| HDF            |
| HFB            |
| HOP            |
| INR            |
| JNS            |
| LIQUORCO       |
| LPRC           |
| MWC            |
| NAA            |
| NOSOLCPP       |
| OCC            |
| ODM            |
| PBE            |
| PCD            |
| PCE            |
| PCL            |
| PCL3           |
| PETCO          |
| RBL            |
| SCC            |
| SCP            |
| SCQ            |
| SDR            |
| SED            |
| SEE            |
| SEF            |
| SFA            |
| SFB            |
| SFD            |
| SFK            |
| SFN            |
| SGG            |
| SGQ            |
| SHVR           |
| SKA            |
| SNPBLBS        |
| WBJ            |
| WBK            |
| WBT            |
| WCA            |
| WCA2           |
+----------------+
"""

# Extracting only capitalized letters as separate strings in a list
capitalized_list = [word.strip().replace('|', '').replace(' ', '') for word in text.split('\n') if word.strip().isupper()]


outer_when_query = ""
for code in capitalized_list:
    line=f"CASE WHEN size({code}) == 1 THEN {code}[0] ELSE 0 END AS count_{code}, \n"
    outer_when_query += line
print(outer_when_query)

inner_when_query = ""
for code in capitalized_list:
    line=f"collect_list(a.group_map['{code}']) as {code}, \n"
    inner_when_query += line

schema=""
for code in capitalized_list:
    line=f"count_{code} int, \n"
    schema+=line
print(schema)


serde=":key,stats:total_count,stats:total_population"
for code in capitalized_list:
    line=f"stats:count_{code},"
    serde+=line
print(serde)
