#importer les packages
import requests
import pandas as pd
from code_utils.matcherANR import address,name,description,identifie_structure,identifie_personne,identifiant_prefere,replace_all,nettoie_scanR,orcid_to_idref,recup_id_personne,persons
from tqdm import tqdm
import pprint as pp
tqdm.pandas()
from code_utils.pickle import load_cache,write_cache
import os
from dotenv import load_dotenv
import json

load_dotenv()

Authorization = os.getenv('Authorization')

########################################### amener les partenaires depuis le site de l'anr #########################################################
url_partenaires_10="https://www.data.gouv.fr/fr/datasets/r/9b08ee21-7372-47a4-9831-4c56a8099ee8"
page_partenaires_10 = requests.get(url_partenaires_10).json()
colonnes_partenaires_10 = page_partenaires_10['columns']
donnees_partenaires_10 = page_partenaires_10['data']
df_partenaires=pd.DataFrame(data=donnees_partenaires_10,columns=colonnes_partenaires_10)
df_partenaires['index']=[x for x in range(len(df_partenaires))]
df_partenaires=df_partenaires.set_index('index')

########################################### RECUP2RATION DES IDENTIFIANTS DE STRUCTURES ET DE PERSONNE #########################################################

############cache structures, personnes et orcid avec differentes sources de donnees
cached_anr_data = {}
try:
    cached_anr_data = load_cache(cached_anr_data,'cached_anr_data.pkl')
except:
    write_cache(cached_anr_data,'cached_anr_data.pkl')
    
cached_anr_data_persons = {}
try:
    cached_anr_data_persons = load_cache(cached_anr_data_persons,'cached_anr_data_persons.pkl')
except:
    write_cache(cached_anr_data_persons,'cached_anr_data_persons.pkl')
    
cached_anr_data_orcid = {}
try:
    cached_anr_data_orcid = load_cache(cached_anr_data_orcid,'cached_anr_data_orcid.pkl')
except:
    write_cache(cached_anr_data_orcid,'cached_anr_data_orcid.pkl')

######## matcher affiliations ==> id_structure
id_struct=df_partenaires
id_struct['Projet.Partenaire.Nom_organisme2']=id_struct.loc[:,'Projet.Partenaire.Nom_organisme'].apply(lambda x: replace_all(str(x).lower().replace(" d e"," d'e").replace(" d a"," d'a").replace(" d i"," d'i").replace(" d o"," d'o").replace(" d u"," d'u").replace(" d y"," d'y").replace(" d h"," d'h").replace(" l e"," l'e").replace(" l a"," l'a").replace(" l i"," l'i").replace(" l o"," l'o").replace(" l u"," l'u").replace(" l y"," l'y").replace(" l h"," l'h")))
id_struct=id_struct.drop_duplicates(subset=['Projet.Partenaire.Nom_organisme2'])
id_struct.progress_apply(lambda row: identifie_structure(row,cached_anr_data), axis=1) #quelques minutes
write_cache(cached_anr_data,'cached_anr_data.pkl')
id_struct['id_structure_matcher']=id_struct.loc[:,'Projet.Partenaire.Nom_organisme'].apply(lambda x: cached_anr_data[x])
id_struct.to_excel('df_partenaires.xlsx')
id_struct.to_json('df_partenaires.json')

#id_struct=pd.read_json('df_partenaires.json')
id_struct=id_struct[['Projet.Partenaire.Nom_organisme','id_structure_matcher']]
id_struct['Projet.Partenaire.Nom_organisme2']=id_struct.loc[:,'Projet.Partenaire.Nom_organisme'].apply(lambda x: replace_all(str(x).lower().replace(" d e"," d'e").replace(" d a"," d'a").replace(" d i"," d'i").replace(" d o"," d'o").replace(" d u"," d'u").replace(" d y"," d'y").replace(" d h"," d'h").replace(" l e"," l'e").replace(" l a"," l'a").replace(" l i"," l'i").replace(" l o"," l'o").replace(" l u"," l'u").replace(" l y"," l'y").replace(" l h"," l'h")))

df_partenaires['Projet.Partenaire.Nom_organisme2']=df_partenaires.loc[:,'Projet.Partenaire.Nom_organisme'].apply(lambda x: replace_all(str(x).lower().replace(" d e"," d'e").replace(" d a"," d'a").replace(" d i"," d'i").replace(" d o"," d'o").replace(" d u"," d'u").replace(" d y"," d'y").replace(" d h"," d'h").replace(" l e"," l'e").replace(" l a"," l'a").replace(" l i"," l'i").replace(" l o"," l'o").replace(" l u"," l'u").replace(" l y"," l'y").replace(" l h"," l'h")))
df_partenaires_struct=pd.merge(df_partenaires,id_struct[['Projet.Partenaire.Nom_organisme2','id_structure_matcher']], on='Projet.Partenaire.Nom_organisme2', how='left')
df_partenaires_struct

#######scanR ==> id_structure_scanr
url_scanr='https://storage.gra.cloud.ovh.net/v1/AUTH_32c5d10cb0fe4519b957064a111717e3/scanR/projects.json'
#recuperation du dataframe avec id_structure_scanr et Projet.Partenaire.Nom_organisme sur scanr 
requete_scanR = requests.get(url_scanr)
page_scanR= requete_scanR.json()
df_scanR=pd.DataFrame(page_scanR)
scanR=df_scanR.explode('participants').loc[:,['id','participants']]
scanR=scanR.rename(columns={'id':'id_anr'})
scanR['index']=[x for x in range(len(scanR))]
scanR=scanR.set_index('index')
scanR['id_structure_scanr']=scanR['participants'].apply(lambda x: x.get(str('structure')) if isinstance(x, dict) else None )
scanR['nom_struct']=scanR['participants'].apply(lambda x: nettoie_scanR(x))
del scanR['participants']
scanR_nettoye=scanR.drop_duplicates(subset='nom_struct')
scanR_nettoye['Projet.Partenaire.Nom_organisme2']=scanR_nettoye.loc[:,'nom_struct'].apply(lambda x: replace_all(str(x).lower()))
scanR_nettoye=scanR_nettoye[['id_structure_scanr','Projet.Partenaire.Nom_organisme2']]
scanR_nettoye=scanR_nettoye.drop_duplicates(subset='Projet.Partenaire.Nom_organisme2')

df_partenaires_struct=pd.merge(df_partenaires_struct,scanR_nettoye, on='Projet.Partenaire.Nom_organisme2', how='left')
df_partenaires_struct

#######fichier avec les identifiants structures rettrouvés à la main par Emmanuel ==> 'code'
scanr_part_nn_id=pd.read_excel('scanr_partenaires_non_identifies.xlsx')
scanr_part_nn_id['Projet.Partenaire.Nom_organisme2']=scanr_part_nn_id.loc[:,'Nom'].apply(lambda x: replace_all(str(x).lower()))
scanr_part_nn_id=scanr_part_nn_id[['Projet.Partenaire.Nom_organisme2','code']]
scanr_part_nn_id=scanr_part_nn_id.drop_duplicates(subset='Projet.Partenaire.Nom_organisme2')
repechage=pd.merge(df_partenaires_struct,scanr_part_nn_id, on='Projet.Partenaire.Nom_organisme2', how='left')
repechage
repechage['index']=[x for x in range(len(repechage))]
repechage=repechage.set_index('index')

#######rassemblement
repechage['id_structure']=repechage.progress_apply(identifiant_prefere, axis=1)
repechage
repechage.to_excel('df_partenaires_id_structures.xlsx')
repechage.to_json('df_partenaires_id_structures.json')

########récupération des structures sans identifiants pour les donner à Emmanuel
repechage2=repechage.loc[(pd.isna(repechage['id_structure']))|(str(repechage['id_structure'])=='None')|(str(repechage['id_structure'])=='nan')]
repechage2
repechage2=repechage2.drop_duplicates(subset='Projet.Partenaire.Nom_organisme2')
repechage2=repechage2[['Projet.Partenaire.Nom_organisme','Projet.Partenaire.Adresse.Ville', 'Projet.Partenaire.Adresse.Region','Projet.Partenaire.Adresse.Pays','id_structure']]
repechage2.to_excel('partenaires_non_identifies.xlsx')

############matcher checheurs
df_partenaires=pd.read_json('df_partenaires_id_structures.json')
df_partenaires['id_personne']=df_partenaires.progress_apply(lambda row: identifie_personne(row, cached_anr_data_persons), axis=1)#environ 5h
write_cache(cached_anr_data_persons,'cached_anr_data_persons.pkl')
df_partenaires.to_excel('df_partenaires_id_personne.xlsx')
df_partenaires.to_json('df_partenaires_id_personne.json')

####récupérer des idref supplémentaires grâce aux ids ORCID fournit par l'ANR
df_partenaires=pd.read_json('df_partenaires_id_personne.json')
df_partenaires['idref_ORCID']=df_partenaires.progress_apply(lambda row: orcid_to_idref(row,cached_anr_data_orcid), axis=1)#environ 1h
write_cache(cached_anr_data_orcid,'cached_anr_data_orcid.pkl')
df_partenaires['id_person']=df_partenaires.apply(lambda row: recup_id_personne(row), axis=1)
df_partenaires.to_json('df_partenaires_id_person_ORCID.json', orient='records')
df_partenaires.to_excel('df_partenaires_id_person_ORCID.xlsx')






############################################################ ENVOI DES PROJETS SUR SCANR #########################################################

df_partenaires=pd.read_json('C:/Users/haallat/Documents/ANR/df_partenaires_id_person_ORCID.json')
df_partenaires.loc[:,'id_structure']=df_partenaires.loc[:,'id_structure'].apply(lambda x: x[0] if isinstance(x,list) else x )
df_partenaires['persons']=df_partenaires.progress_apply(lambda row: persons(row) ,axis=1)
df_partenaires=df_partenaires.groupby(['Projet.Code_Decision_ANR']).agg({'persons': lambda x: x.tolist()}).reset_index()

######## amener les projets depuis le site de l'anr
url_projets_10="https://www.data.gouv.fr/fr/datasets/r/afe3d11b-9ea2-48b0-9789-2816d5785466"
page_projets_10 = requests.get(url_projets_10).json()
colonnes_projets_10 = page_projets_10['columns']
donnees_projets_10 = page_projets_10['data']
df_projets=pd.DataFrame(data=donnees_projets_10,columns=colonnes_projets_10)
df_projets['index']=[x for x in range(len(df_projets))]
df_projets=df_projets.set_index('index')

#######on complete les projets de l'anr avec les personnes qui ont participé au projet
df_projets=pd.merge(df_projets,df_partenaires,on='Projet.Code_Decision_ANR', how='left')
df_projets['type']="ANR"
df_projets['name']=df_projets.progress_apply(lambda row: name(row) ,axis=1)
df_projets['description']=df_projets.progress_apply(lambda row: description(row) ,axis=1)
df_projets=df_projets.rename(columns={'Projet.Code_Decision_ANR': 'id', 'Projet.Acronyme': 'acronym', 'AAP.Edition':'year', 'Projet.Montant.AF.Aide_allouee.ANR':'budget_financed'})
df_projets=df_projets[['id','type','name','description','acronym','year','budget_financed','persons']]

############################################################ POUR METTRE A JOUR LES NOUVEAUX PROJETS ANR - PROJETS #########################################################

nbr_page=int(requests.get('http://185.161.45.213/projects/projects?where={"type":"ANR"}&projection={"id":1}&max_results=500&page=1', headers={"Authorization":Authorization}).json()['hrefs']['last']['href'].split('page=')[1])

list_ids=[]
for i in range(1,nbr_page+1):
    print("page",i)
    page=requests.get('http://185.161.45.213/projects/projects?where={"type":"ANR"}&projection={"id":1}&max_results=500'+f"&page={i}", headers={"Authorization":Authorization}).json()
    for k in range(len(page['data'])):
        print("k",k)
        list_ids.append(page['data'][k]['id'])
    
projets_a_ajouter=[x for x in list(df_projets['id']) if x not in list_ids]

projets_a_retirer=[x for x in list_ids if x not in list(df_projets['id'])]

df_projets = df_projets[df_projets['id'].apply(lambda x: x in projets_a_ajouter)]

##envoi
err=[]
for i,row in df_projets.iterrows():
    dict_row=row.to_dict()
    dict_row2={k:v for k,v in list(dict_row.items()) if ((str(v)!='nan')&(str(v)!='NaN')&(str(v)!='None')&(str(v)!='x'))}
    try:
       r=requests.post('http://185.161.45.213/projects/projects', json = dict_row2, headers={"Authorization":Authorization})
       res= r.json()
       if res.get('status')=='ERR':
           err.append(res)
           if res.get('error').get('code')!=422:
               print(err)
               pp.pprint(err)
    except Exception as e:
        pp.pprint(e)
        
#pd.Series([x.get('issues').get('id')[25:] for x in err]).drop_duplicates().tolist() #si jamais il y a des erreurs








############################################################ ENVOI DES PARTENAIRES SUR SCANR #########################################################

df_partenaires=pd.read_json('df_partenaires_id_structures.json')
df_partenaires['address']=df_partenaires.apply(lambda row: address(row), axis=1)
df_partenaires=df_partenaires[['Projet.Partenaire.Nom_organisme','Projet.Partenaire.Code_Decision_ANR','Projet.Code_Decision_ANR','id_structure','Projet.Partenaire.Est_coordinateur','address']]
df_partenaires=df_partenaires.rename(columns={'Projet.Partenaire.Code_Decision_ANR': 'id','Projet.Partenaire.Nom_organisme': 'name', 'Projet.Code_Decision_ANR': 'project_id', 'id_structure':'participant_id'})
df_partenaires['role']=df_partenaires['Projet.Partenaire.Est_coordinateur'].apply(lambda x: 'coordinator' if str(x) == 'True' else 'participant')
df_partenaires['project_type']='ANR'
df_partenaires['participant_id']=df_partenaires.loc[:,'participant_id'].apply(lambda x: x[0] if isinstance(x,list) else str(x).split(';')[0])
df_partenaires=df_partenaires[['id','project_id', 'project_type', 'participant_id', 'role', 'name','address']]
df_partenaires

############################################################ POUR METTRE A JOUR LES NOUVEAUX PROJETS ANR - PARTENAIRES #########################################################

# n=len(df_partenaires)//100
# list_100_by_100=[int(f"{i}00") for i in range(0,n+1)]
# index_list=list_100_by_100+[list_100_by_100[-1]+(len(df_partenaires)%100)+1]
# dfs_partenaires=[df_partenaires.iloc[index_list[i]:index_list[i+1],:] for i in range(n+1)]

nbr_page=int(requests.get('http://185.161.45.213/projects/participations?where={"project_type":"ANR"}&projection={"project_id":1}&max_results=500&page=1', headers={"Authorization":Authorization}).json()['hrefs']['last']['href'].split('page=')[1])

list_ids=[]
for i in range(1,nbr_page+1):
    print("page",i)
    page=requests.get('http://185.161.45.213/projects/participations?where={"project_type":"ANR"}&projection={"project_id":1}&max_results=500'+f"&page={i}", headers={"Authorization":Authorization}).json()
    for k in range(len(page['data'])):
        print("k",k)
        list_ids.append(page['data'][k]['project_id'])
        
# projets_a_ajouter=[]    
# for df in dfs_partenaires:
#     list_anr_ids=list(df['Projet.Code_Decision_ANR']) 
#     projets_a_ajouter.append([x for x in list_anr_ids if x not in list_ids])
    
projets_a_ajouter=[x for x in list(df_partenaires['project_id'].drop_duplicates()) if x not in list(pd.Series(list_ids).drop_duplicates())]

projets_a_retirer=[x for x in list_ids if x not in list(df_partenaires['project_id'])]

df_partenaires = df_partenaires[df_partenaires['project_id'].apply(lambda x: x in projets_a_ajouter)]

dict_row=df_partenaires.iloc[0,:].to_dict()
print(dict_row)
print({k:v for k,v in list(dict_row.items()) if ((str(v)!='nan')&(str(v)!='NaN')&(str(v)!='None')&(str(v)!='x'))})

###envoi
err=[]
for i,row in df_partenaires.iterrows():
    dict_row=row.to_dict()
    dict_row2={k:v for k,v in list(dict_row.items()) if ((str(v)!='nan')&(str(v)!='NaN')&(str(v)!='None')&(str(v)!='x'))}
    try:
       r=requests.post('http://185.161.45.213/projects/participations', json = dict_row2, headers={"Authorization":Authorization})
       res= r.json()
       if res.get('status')=='ERR':
           err.append(res)
           if res.get('error').get('code')!=422:
               print(err)
               pp.pprint(err)
    except Exception as e:
        pp.pprint(e)

#pd.Series([x.get('issues').get('id')[25:] for x in err]).drop_duplicates().tolist() #si jamais il y a des erreurs



############################################ Trouver les structures qui font des projets IA ###################################################

def ia_proj(title_resume):
    return any(mot for mot in IA_keywords if mot in str(title_resume).lower())

df_partenaires=pd.read_json('df_partenaires_id_structures.json')

######## amener les projets depuis le site de l'anr
url_projets_10="https://www.data.gouv.fr/fr/datasets/r/afe3d11b-9ea2-48b0-9789-2816d5785466"
page_projets_10 = requests.get(url_projets_10).json()
colonnes_projets_10 = page_projets_10['columns']
donnees_projets_10 = page_projets_10['data']
df_projets=pd.DataFrame(data=donnees_projets_10,columns=colonnes_projets_10)
df_projets['index']=[x for x in range(len(df_projets))]
df_projets=df_projets.set_index('index')

df_projets=pd.merge(df_projets,df_partenaires,on='Projet.Code_Decision_ANR', how='left')

IA_keywords=['machine learning','learning machine','intelligence artificielle','artificial intelligence',' ai-',' ia-',' ai ',' ia ']
df_projets['title_resume']=df_projets['Projet.Titre.Anglais']+' '+df_projets['Projet.Resume.Anglais']
df_projets['oriente_ia']=df_projets['title_resume'].apply(lambda x: ia_proj(x))
print(len(df_projets[(df_projets['oriente_ia'])&(df_projets['AAP.Edition']>2018)&(df_projets['AAP.Edition']<2024)])/len(df_projets[(df_projets['AAP.Edition']>2018)&(df_projets['AAP.Edition']<2024)]))

df_projets_ia=df_projets[(df_projets['oriente_ia'])&(df_projets['AAP.Edition']>2018)&(df_projets['AAP.Edition']<2024)]

df_projets_ia[['Projet.Code_Decision_ANR','Projet.Acronyme','id_structure','persons']].to_excel('df_partenaires_ia.xlsx')



