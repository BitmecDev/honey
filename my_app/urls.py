from django.urls import path

from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('send-data/', views.SendHoneyCombData.as_view(), name='send-data'),
    path('deactivate-emergency', views.SendDeactivateEmergencySocketMessage.as_view(), name='deactivate-emergency'),
]