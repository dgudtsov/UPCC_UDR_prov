'''
Created on 6 янв. 2023 г.

@author: denis
'''

xml_custom = {
'Custom1':'',
'Custom2':'',
'Custom3':'',
'Custom4':'',
'Custom5':'',
'Custom6':'',
'Custom7':'',
'Custom8':'',
'Custom9':'',
'Custom10':'',
'Custom11':'',
'Custom12':'',
'Custom13':'',
'Custom14':'',
'Custom15':'',
'Custom16':''    
    }

# =CONCAT("<field name=";B1;">";"{";B1;"}";"</field>")

# <field name=Custom1>{Custom1}</field>
# <field name=Custom2>{Custom2}</field>
# <field name=Custom3>{Custom3}</field>
# <field name=Custom4>{Custom4}</field>
# <field name=Custom5>{Custom5}</field>
# <field name=Custom6>{Custom6}</field>
# <field name=Custom7>{Custom7}</field>
# <field name=Custom8>{Custom8}</field>
# <field name=Custom9>{Custom9}</field>
# <field name=Custom10>{Custom10}</field>
# <field name=Custom11>{Custom11}</field>
# <field name=Custom12>{Custom12}</field>
# <field name=Custom13>{Custom13}</field>
# <field name=Custom14>{Custom14}</field>
# <field name=Custom15>{Custom15}</field>
# <field name=Custom16>{Custom16}</field>

xml_template_custom="<field name={Custom_Name}>{Custom_Value}</field>"

xml_template_subs="""
<txRequest id="1">
<createSubscriber>
<key>
<MSISDN>{MSISDN}</MSISDN>
<IMSI>{IMSI}</IMSI>
</key>
<entity>
<data>
<name>Subscriber</name>
<interface>XMLIMPORT</interface>
<xpath/>
</data>
<content>
<![CDATA[<?xml version="1.0" encoding="UTF-8"?>
<subscriber>
<field name="MSISDN">{MSISDN}</field>
<field name="IMSI">{IMSI}</field>
{CUSTOM}
</subscriber>
]]>
</content>
</entity>
</createSubscriber>
</txRequest>
""".replace("\n", "")

# Delete subscribers
xml_template_delete_subs="""
<deleteSubscriber>
<key>
<MSISDN>{KEY}</MSISDN>
</key>
</deleteSubscriber>
""".replace("\n", "")

xml_template_replace_subs="<transaction><txRequest id=\"1\">"+xml_template_delete_subs+"</txRequest><txRequest id=\"2\">"+xml_template_subs+"</txRequest></transaction>"

xml_template=dict()

xml_template = {
    'delete' : xml_template_delete_subs
    ,'create_subs' : xml_template_subs
    ,'replace_subs': xml_template_replace_subs
    }
