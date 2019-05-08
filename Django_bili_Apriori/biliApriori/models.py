from django.db import models

# Create your models here.
class output_sup(models.Model):
    items_s=models.CharField(max_length=32)
    freq=models.CharField(max_length=32)
class output_con(models.Model):
    antecedent_s= models.CharField(max_length=32)
    consequent_s= models.CharField(max_length=32)
    confidence= models.CharField(max_length=32)
    lift= models.CharField(max_length=32)
class default_sup(models.Model):
    items_s=models.CharField(max_length=32)
    freq=models.CharField(max_length=32)
class default_con(models.Model):
    antecedent_s= models.CharField(max_length=32)
    consequent_s= models.CharField(max_length=32)
    confidence= models.CharField(max_length=32)
    lift= models.CharField(max_length=32)
