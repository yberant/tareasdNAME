integrantes grupo "pero ella me levantó":
	- Diego Valderas, rol: 201673549-6
	- Yoel Berant, rol: 201604519-8
	
Distribución de entidades:
	- namenode: dist89
	- datanode: dist90, dist91 y dist92
	- client: dist 89, dist90, dist91 y dist92
	
Comandos de Makefile (ejecutables desde el mismo directorio en el que se encuentra este README):
	- make cli: ejecutar un proceso cliente
	- make data: ejecutar un proceso datanode
	- make name: ejecutar un proceso namenode

Instrucciones y "seteo" del sistema:
	- Lo primero que hay que hacer es ejecutar y conectar el namenode y los tres datanodes.
	- Cuando un datanode empieza su ejecución, se preguntará por consola su "id", un número que puede ser "1", "2" y "3". A cada uno de los datanodes se le debe ingresar un id distinto, cosa que se "diferencien" (este id sirve, entre otras cosas, para dictaminar las prioridades en el algoritmo de Ricart/Agrawala implementado).
	- Siguiendo con el punto anterior, se aconseja asignarle el id "1" al datanode que se ejecuta en la máquina "dist90", "2" al de la "dist91" y "3" al de la "dist92", puesto que las direcciones de ip de las cuatro máquinas (incluyendo a dist89) difieren solo por su último número: la de dist89 termina en 0, dist90 en 1, dist91 en 2 y dist92 en 3. De esta forma, cuando se pidan las direcciones IP de las entidades por consola será más fácil, pues es copiar una sola IP, pegar y modificar el último número por "1", "2" o "3"; según el id del datanode cuya dirección se pida, o por "0" si se pide la dirección del namenode.
	- Como se puede inferir del punto anterior, es necesario conectar el namenode y los datanodes entre sí. Para eso, en cada proceso se pedirá ingresar la dirección IP de cada una de las máquinas en la que se ejecutan los otros procesos. Por ejemplo, para el datanode 1, se pedirá la dirección IP del datanode 2, el datanode 3 y el namenode. 
	No es necesario ingresar los puertos en este caso, pues están fijos (números del 8993 al 8999).
	- Una vez se hayan conectado las cuatro entidades (3 datanodes y un namenode) entre sí, en cada una se podrá ingresar por pantalla uno de los siguientes comantos:
		* listen: se pedirá ingresar un número de puerto para escuchar a un cliente
		* quit: se termina la ejecución del proceso y se imprime la cantidad de mensajes enviados por este (importante para la realización del informe)
	- Para ingresar a un cliente al sistema, los 3 datanodes y el namenode deben "escucharlo". Para eso se debe ocupar el comando listen en estos cuatro procesos, ingresando en cada uno un número de puerto para escuchar al cliente. Dado que estos cuatro procesos están en máquinas distintas (y por tanto, sus direcciones IP varían), el número de puerto puede ser el mismo entre los cuatro procesos, siempre que no sean valores del 8993 al 8999, pues estos fueron reservados para las conexiones entre los 4 procesos. 
	- Una vez los 3 datanodes y el namenode estén escuchando al ciente, se puede ejecutar un proceso cliente e ingresar las direcciones IP del namenode y los datanodes; y los puertos desde los cuales estos procesos están escuchando al cliente.
	- Si todo se hizo bien, el cliente podrá ingresar por pantalla 3 números. Cada número es un comando:
		* "1" - subir archivo: se debe ingresar el nombre del archivo libro que se desea "subir" al sistema, para que sea distribuído. Los archivos que se ingresan deben estar en la carpeta "filesup", en directiorio "client" y se deben ingresar con su extensión (".pdf"). Los archivos son:
			- Don_Quijote_de_la_Mancha-Cervantes_Miguel.pdf
			- Dracula-Stoker_Bram.pdf
			- El_cuervo-Allan_Poe_Edgar.pdf
			- El_maravilloso_Mago_de_Oz-L._Frank_Baum.pdf
			- La_isla_del_tesoro-Robert_Louis_Stevenson.pdf
			- La_vuelta_al_mundo_en_80_dias-Verne_Julio.pdf
			- Los_Miserables-Hugo_Victor.pdf
			- Moby_Dick-Herman_Melville.pdf
			- Oliver_Twist-Charles_Dickens.pdf
			- Orgullo_y_prejuicio-Jane_Austen.pdf    
		Las partes de los archivos, distribuídas en un datanode, podrán ser encontradas en las carpeta "files" dentro del directorio "datanode" (solo en las máquinas que contienen código de datanode). A su vez, el regitro de chunks del namenode se encontrará en el archivo "log.txt", presente en el directorio "namenode" (solo en la máquina que contiene al namenode).
		* "2" - descargar archivo: lo inverso a la opción anterior. Se debe ingresar el nombre de un archivo que ya se haya ingresado antes al sistema para descargarlo. Este aparecerá en la carpeta "filesdown", en el directorio "client", en la máquina desde la cual está corriendo el proceso cliente.
		
		* "3" - salir: esta se explica por si sola.
		
		
