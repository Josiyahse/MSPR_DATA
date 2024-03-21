# MSPR_DATA
Projet visant à créer un traitement et un traitement pour de la prédiction du prochain président de la république

Si voue êtes sur window ou mac, il faudra créer un fichier de configuration pour votre container.
Sur ces machine les spec sont limité par defaut et elles empêche Airflow de fonctionner. 
Céer un ficher dans : C:\Users\<Utillisateur>\.wslconfig

🚨🚨 Cette configuration est global pour votre Docker. Si vou souhaiter configurer qu'un seul container il faut utiliser le fichier wsl.conf directment present dans le container. Plus sur [text](https://learn.microsoft.com/en-us/windows/wsl/wsl-config#wslconf)

Et voicis un exemple de configuration:
```bash:
# Settings apply across all Linux distros running on WSL 2
[wsl2]

# Limits VM memory to use no more than 4 GB, this can be set as whole numbers using GB or MB
memory=8GB 

# Sets the VM to use two virtual processors
processors=4

# Sets amount of swap storage space to 8GB, default is 25% of available RAM
swap=4GB

# Sets swapfile path location, default is %USERPROFILE%\AppData\Local\Temp\swap.vhdx
swapfile=C:\\temp\\wsl-swap.vhdx

# Enable experimental features
[experimental]
sparseVhd=true
```
