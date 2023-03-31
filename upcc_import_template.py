'''
Created on 6 янв. 2023 г.

@author: denis
'''

# =CONCAT("<field name=";B1;">";"{";B1;"}";"</field>")

xml_template_custom="""
<field name="{Custom_Name}">{Custom_Value}</field>
""".replace("\n", "")

xml_template_entitlement="""
<field name="Entitlement">{Entitlement}</field>
""".replace("\n", "")

xml_template_begin_transact="<transaction>"
xml_template_end_transact="</transaction>"

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
<field name="BillingDay">{BillingDay}</field>
{ENTITLEMENT}
{CUSTOM}
</subscriber>
]]>
</content>
</entity>
</createSubscriber>
</txRequest>
""".replace("\n", "")

#<txRequest id="{REQ}">
xml_template_quota="""
<txRequest id="1">
<create createEntityIfNotExist="true">
<key>
<IMSI>{IMSI}</IMSI>
</key>
<entity>
<data>
<name>Quota</name>
<interface>XMLIMPORT</interface>
<xpath/>
</data>
<content>
<![CDATA[<?xml version="1.0" encoding="UTF-8"?>
<usage>
<version>3</version>
{QUOTA}
</usage>
]]>
</content>
</entity>
</create>
</txRequest>
""".replace("\n", "")

# Dynamic Quota definition
xml_template_dyn_quota="""
<txRequest id="1">
<create createEntityIfNotExist="true">
<key>
<IMSI>{IMSI}</IMSI>
</key>
<entity>
<data>
<name>DynamicQuota</name>
<interface>XMLIMPORT</interface>
<xpath/>
</data>
<content>
<![CDATA[<?xml version="1.0" encoding="UTF-8"?>

<Definition>
<version>1</version>
{QUOTA}
</Definition>
]]>
</content>
</entity>
</create>
</txRequest>
""".replace("\n", "")

# static (regular) quota usage definition
xml_template_quota_usage="""
<quota name="{QUOTA}">
<totalVolume>{VOLUME}</totalVolume>
<Type>quota</Type>
</quota>
""".replace("\n", "")

# Dynamic Quota configuration definition (usage is defined the same way as regular quota)
xml_template_quota_topup="""
<DynamicQuota name="{QUOTA}">
<InstanceId>{INSTANCE}</InstanceId>
<Type>top-up</Type>
<InitialTotalVolume>{VOLUME}</InitialTotalVolume>
</DynamicQuota>
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
    ,'create_quota' : xml_template_quota
    ,'create_dquota' : xml_template_dyn_quota
    ,'quota_usage' : xml_template_quota_usage
    ,'topup_quota' : xml_template_quota_topup
    ,'replace_subs': xml_template_replace_subs
    }
