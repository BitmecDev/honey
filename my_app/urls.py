from django.urls import path

from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('send-data/', views.SendHoneyCombData.as_view(), name='send-data'),
    path('height/', views.SendHeightSocketMessage.as_view(), name='height'),
    path('weight/', views.SendWeightSocketMessage.as_view(), name='weight'),
    path('esfigmo/', views.SendPressureSocketMessage.as_view(), name='esfigmo'),
    path('oxygen/', views.SendOxygenSocketMessage.as_view(), name='oxygen'),
    path('oxygen/', views.SendOxygenSocketMessage.as_view(), name='oxygen'),
    path('temperature/', views.SendTemperatureSocketMessage.as_view(), name='temperature'),
    path('stethoscope/', views.SendEstetoSocketMessage.as_view(), name='stethoscope'),
    path('patient-exit/', views.SendPatientExitSocketMessage.as_view(), name='patient-exit'),
    path('activate-emergency/', views.SendActivateEmergencySocketMessage.as_view(), name='activate-emergency'),
    path('deactivate-emergency/', views.SendDeactivateEmergencySocketMessage.as_view(), name='deactivate-emergency'),
]