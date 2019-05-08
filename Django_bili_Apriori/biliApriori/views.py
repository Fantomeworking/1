import datetime
import os
from django.shortcuts import render
from biliApriori import models
from django.db import connection

cursor=connection.cursor()

def cheakout(request):
    if request.method=='POST':
        Support = request.POST.get("Support", None)#支持度
        Confidence = request.POST.get("Confidence", None)#置信度
        numPartitions = request.POST.get("numPartitions", None)  # 数据分区
        min_views=request.POST.get("min-views",None)#观看量筛选
        max_views = request.POST.get("max-views", None)  # 观看量筛选
        min_time = request.POST.get("min-time", None)#时间筛选
        max_time = request.POST.get("max-time", None)  # 时间筛选

        if Support!='' or Confidence!='' or numPartitions!='' or min_views!='' or max_views!='' or min_time!='' or max_time!='':
            Support='0.03'*(Support=="")+Support*(Support!="")
            Confidence = '0.03' * (Confidence =="") + Confidence * (Confidence !="")
            numPartitions = '5' * (numPartitions =="") + numPartitions * (numPartitions !="")
            min_views = '10000' * (min_views =="") + min_views * (min_views != "")
            max_views = '1000000000' * (max_views == "") + max_views * (max_views != "")
            min_time = '3' * (min_time == "") + min_time * (min_time != "")
            max_time = '7' * (max_time == "") + max_time * (max_time != "")
            nowUTCtime_date = datetime.datetime.utcnow()
            now_UTCtimestamp = datetime.datetime.timestamp(nowUTCtime_date)
            min_outtime = now_UTCtimestamp - int(min_time) * 86400  #下限天数，默认3天
            max_outtime = now_UTCtimestamp - int(max_time)*86400#上限天数，默认7天
            os.system('$SPARK_HOME/bin/spark-submit \
                        --class cn.study.spark.apriori.bili_Apriori \
                        /home/fantome/Projects/IdeaProjects/spark_streaming/out/artifacts/spark_streaming_jar/spark_streaming.jar \
                        "'+Support+'" "'+Confidence+'" "'+numPartitions+'" "'+min_views+'" "'+max_views+'" "'+str(int(min_outtime))+'" "'+str(int(max_outtime))+'"')
            output_sup=models.output_sup.objects.order_by("-freq").all()
            output_con = models.output_con.objects.order_by("-confidence").all()
            return render(request,'cheakout.html',{'data_sup':output_sup,'data_con':output_con})
        else:
            default_sup = models.default_sup.objects.order_by("-freq").all()
            default_con = models.default_con.objects.order_by("-confidence").all()
            return render(request, 'cheakout.html', {'data_sup': default_sup, 'data_con': default_con})
    else:
        default_sup = models.default_sup.objects.order_by("-freq").all()
        default_con = models.default_con.objects.order_by("-confidence").all()
        return render(request, 'cheakout.html', {'data_sup': default_sup, 'data_con': default_con})
