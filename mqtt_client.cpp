#include <iostream>
#include <sstream>
#include <iomanip>
#include <mosquittopp.h>
#include <csignal>
#include <thread>
#include <string>
#include <optional>
#include "nlohmann/json.hpp"
#include <ctime>
#include <mysql.h>
#include <memory>
#include <chrono>
#include <unistd.h>
#include <cstring>

// Utilisation de la bibliothèque json de nlohmann pour le parsing JSON
using json = nlohmann::json;
using namespace std;

// Classe pour gérer la connexion à la base de données MySQL
class GestionnaireBaseDeDonnees
{
public:
    // Constructeur pour initialiser la connexion à la base de données
    GestionnaireBaseDeDonnees(const string& hoteBD, const string& utilisateurBD, const string& motDePasseBD, const string& nomBD)
        : hoteBD_(hoteBD), utilisateurBD_(utilisateurBD), motDePasseBD_(motDePasseBD), nomBD_(nomBD),
          mysql_(nullptr, mysql_close)
    {
        connecterBaseDeDonnees(); // Connexion à la base de données lors de l'initialisation
    }

    // Destructeur pour fermer la connexion à la base de données
    ~GestionnaireBaseDeDonnees()
    {
        deconnecterBaseDeDonnees();
    }

    // Méthode pour obtenir les noms des passerelles et leurs protocoles
    vector<pair<string, int>> obtenirNomsPasserellesEtProtocoles();

    // Méthode pour obtenir l'ID d'un dispositif à partir de son nom
    int obtenirIdDispositif(const string& nomPasserelle);

    // Méthode pour insérer des données de mesure dans la base de données
    void insererDonneesMessage(const string& nomPasserelle, time_t horodatageUTC, double differenceKWh, int idDispositif);

private:
    string hoteBD_, utilisateurBD_, motDePasseBD_, nomBD_;
    unique_ptr<MYSQL, decltype(&mysql_close)> mysql_;

    // Méthode pour établir la connexion à la base de données
    void connecterBaseDeDonnees();

    // Méthode pour fermer la connexion à la base de données
    void deconnecterBaseDeDonnees();
};

// Implémentation de la méthode pour obtenir les noms des passerelles et leurs protocoles
vector<pair<string, int>> GestionnaireBaseDeDonnees::obtenirNomsPasserellesEtProtocoles()
{
    vector<pair<string, int>> protocolesPasserelles;
    string requete = "SELECT Nom_dispositif, ID_Protocole_FK FROM Dispositif_Passerelle";
    mysql_query(mysql_.get(), requete.c_str());

    MYSQL_RES* resultat = mysql_store_result(mysql_.get());
    MYSQL_ROW ligne;
    while ((ligne = mysql_fetch_row(resultat)))
    {
        protocolesPasserelles.emplace_back(ligne[0], stoi(ligne[1]));
    }
    mysql_free_result(resultat);
    return protocolesPasserelles;
}

// Implémentation de la méthode pour obtenir l'ID d'un dispositif
int GestionnaireBaseDeDonnees::obtenirIdDispositif(const string& nomPasserelle)
{
    string requete = "SELECT ID_Dispositif_PK FROM Dispositif_Passerelle WHERE Nom_dispositif = '" + nomPasserelle + "' LIMIT 1";
    mysql_query(mysql_.get(), requete.c_str());
    MYSQL_RES* resultat = mysql_store_result(mysql_.get());
    MYSQL_ROW ligne = mysql_fetch_row(resultat);
    if (!ligne)
    {
        mysql_free_result(resultat);
        return -1;
    }
    int idDispositif = stoi(ligne[0]);
    mysql_free_result(resultat);

    return idDispositif;
}

// Implémentation de la méthode pour insérer des données de mesure
void GestionnaireBaseDeDonnees::insererDonneesMessage(const string& nomPasserelle, time_t horodatageUTC, double differenceKWh, int idDispositif)
{
    stringstream fluxRequete;
    fluxRequete << "INSERT INTO Donnee_Mesurer (Timestamp, Valeur_Mesure, ID_Dispositif_FK) VALUES (FROM_UNIXTIME("
                << horodatageUTC << "), " << differenceKWh << ", " << idDispositif << ")";
    mysql_query(mysql_.get(), fluxRequete.str().c_str());
}

// Implémentation de la méthode pour établir la connexion à la base de données
void GestionnaireBaseDeDonnees::connecterBaseDeDonnees()
{
    mysql_.reset(mysql_init(nullptr));
    if (!mysql_real_connect(mysql_.get(), hoteBD_.c_str(), utilisateurBD_.c_str(), motDePasseBD_.c_str(), nomBD_.c_str(), 0, nullptr, 0)) {
        cerr << "Erreur de connexion à la base de données: " << mysql_error(mysql_.get()) << endl;
    } else {
        cout << "Connecté à la base de données." << endl;
    }
}

// Implémentation de la méthode pour fermer la connexion à la base de données
void GestionnaireBaseDeDonnees::deconnecterBaseDeDonnees()
{
    if (mysql_) {
        mysql_close(mysql_.release());
    }
}

// Structure pour stocker les données de la passerelle
struct DonneesPasserelle
{
    string nom;
    double dernieresValeursChargeKwh[7]; 
    double derniereValeurBaseChargeKwh;
    int compteurValeursCharge;
    int compteurMessages; 
    string dernierMessageTraite; 
    int totalMessagesRecus; 

    // Constructeur pour initialiser les données de la passerelle
    DonneesPasserelle(const string& nomPasserelle)
        : nom(nomPasserelle), compteurValeursCharge(0), compteurMessages(0), dernierMessageTraite(""), totalMessagesRecus(0), derniereValeurBaseChargeKwh(0.0)
    {
        for (int i = 0; i < 7; ++i)
        {
            dernieresValeursChargeKwh[i] = 0.0;
        }
    }
};

// Classe pour traiter les messages entrants
class TraitementMessage
{
public:
    // Constructeur pour initialiser le traitement des messages
    TraitementMessage(GestionnaireBaseDeDonnees& gestionnaireBD);

    // Méthode pour traiter un message entrant
    void traiterMessageEntrant(const string& sujet, const string& chargeUtile);

    // Méthode pour afficher un message dans le terminal
    void afficherMessageDansTerminal(const string& chargeUtile);

    // Méthode pour insérer un message dans la base de données
    void insererMessageDansBDD(const string& sujet, const string& chargeUtile);

    // Méthode pour extraire le nom de la passerelle à partir du sujet
    string extraireNomPasserelleDuSujet(const string& sujet);

private:
    GestionnaireBaseDeDonnees& gestionnaireBD_;
    vector<DonneesPasserelle> etatsPasserelles_;
};

// Implémentation du constructeur pour initialiser le traitement des messages
TraitementMessage::TraitementMessage(GestionnaireBaseDeDonnees& gestionnaireBD)
    : gestionnaireBD_(gestionnaireBD) {}

// Implémentation de la méthode pour traiter un message entrant
void TraitementMessage::traiterMessageEntrant(const string& sujet, const string& chargeUtile)
{
    afficherMessageDansTerminal(chargeUtile);
    insererMessageDansBDD(sujet, chargeUtile);
}

// Implémentation de la méthode pour afficher un message dans le terminal
void TraitementMessage::afficherMessageDansTerminal(const string& chargeUtile)
{
    if (json::accept(chargeUtile))
    {
        auto donneesJson = json::parse(chargeUtile);
        if (donneesJson.contains("utctimestamp"))
        {
            time_t valeurTemps = donneesJson["utctimestamp"];
            cout << "Horodatage UTC: " << put_time(gmtime(&valeurTemps), "%Y-%m-%d %H:%M:%S") << endl;
        }
        if (donneesJson["measures"].contains("Load_0_30001") && donneesJson["measures"].contains("Load_0_30000"))
        {
            double valeurChargeKWh = donneesJson["measures"]["Load_0_30001"].get<double>() / 1000.0;
            double valeurBaseChargeKWh = donneesJson["measures"]["Load_0_30000"].get<double>() / 1000.0;
            cout << "Load_0_30001: " << valeurChargeKWh << " kWh" << endl;
            cout << "Load_0_30000: " << valeurBaseChargeKWh << " kWh" << endl;
        }
    }
}

// Implémentation de la méthode pour insérer un message dans la base de données
void TraitementMessage::insererMessageDansBDD(const string& sujet, const string& chargeUtile)
{
    auto donneesJson = json::parse(chargeUtile);

    time_t horodatageUTC = donneesJson["utctimestamp"];
    double valeurChargeActuelleKWh = donneesJson["measures"]["Load_0_30001"].get<double>() / 1000.0;
    double valeurBaseChargeActuelleKWh = donneesJson["measures"]["Load_0_30000"].get<double>() / 1000.0;

    string nomPasserelle = extraireNomPasserelleDuSujet(sujet);
    int idDispositif = gestionnaireBD_.obtenirIdDispositif(nomPasserelle);

    if (idDispositif == -1) return;

    auto it = std::find_if(etatsPasserelles_.begin(), etatsPasserelles_.end(),
                           [&nomPasserelle](const DonneesPasserelle& ep) {
                               return ep.nom == nomPasserelle;
                           });

    if (it == etatsPasserelles_.end())
    {
        etatsPasserelles_.emplace_back(nomPasserelle);
        it = etatsPasserelles_.end() - 1;
    }

    if (chargeUtile == it->dernierMessageTraite)
    {
        cout << "Message déjà traité pour " << nomPasserelle << ", ignoré." << endl;
        cout << "=======================================================" << endl;
        return;
    }

    if (it->compteurValeursCharge < 7)
    {
        it->dernieresValeursChargeKwh[it->compteurValeursCharge++] = valeurChargeActuelleKWh + valeurBaseChargeActuelleKWh;
    }
    else
    {
        for (int i = 0; i < 6; ++i)
        {
            it->dernieresValeursChargeKwh[i] = it->dernieresValeursChargeKwh[i + 1];
        }
        it->dernieresValeursChargeKwh[6] = valeurChargeActuelleKWh + valeurBaseChargeActuelleKWh;
    }

    it->totalMessagesRecus++;
    cout << "Message reçu : " << it->totalMessagesRecus << endl;

    if (it->compteurValeursCharge == 7)
    {
        double diff = it->dernieresValeursChargeKwh[6] - it->dernieresValeursChargeKwh[0];
        it->compteurMessages++;

        if (it->compteurMessages >= 1)
        {
            gestionnaireBD_.insererDonneesMessage(nomPasserelle, horodatageUTC, diff, idDispositif);
            cout << "Données insérées dans la base de données pour " << nomPasserelle << "." << endl;
            cout << "==============================" << endl;

            it->dernieresValeursChargeKwh[0] = it->dernieresValeursChargeKwh[6];
            it->compteurValeursCharge = 1;
            it->compteurMessages = 0;
        }
    }
    else
    {
        cout << "Pas assez de valeurs pour " << nomPasserelle << ", insertion ignorée." << endl;
        cout << "===================================================" << endl;
    }

    it->dernierMessageTraite = chargeUtile;
}

// Implémentation de la méthode pour extraire le nom de la passerelle à partir du sujet
string TraitementMessage::extraireNomPasserelleDuSujet(const string& sujet)
{
    size_t debut = sujet.find("/", 18) + 1;
    size_t fin = sujet.find("/", debut);
    return sujet.substr(debut, fin - debut);
}

// Classe pour gérer le client MQTT
class ClientMQTT : public mosqpp::mosquittopp
{
public:
    ClientMQTT(const string& idClient, const string& hoteMQTT, int portMQTT, GestionnaireBaseDeDonnees& gestionnaireBD, const string& nomUtilisateurMQTT, const string& motDePasseMQTT, char* argv[]);

    ~ClientMQTT();

    // Méthode appelée lors de la connexion au broker MQTT
    void on_connect(int rc) override;

    // Méthode appelée lors de la réception d'un message MQTT
    void on_message(const struct mosquitto_message* message) override;

    chrono::system_clock::time_point obtenirDernierTempsMessage() const;

    void rafraichirAbonnementsPasserelles();

    void verifierChangementsPasserelles();

private:
    string hoteMQTT_;
    int portMQTT_;
    GestionnaireBaseDeDonnees& gestionnaireBD_;
    TraitementMessage processeurMessage_;
    chrono::system_clock::time_point dernierTempsMessage_;
    string nomUtilisateurMQTT_;
    string motDePasseMQTT_;
    char** argv_;
    bool estEnRafraichissementAbonnements_;
    static vector<pair<string, int>> passerellesPrecedentes;

    void connecterAuBrokerMQTT();

    void demarrerBoucleMQTT();

    void arreterBoucleMQTT();

    void deconnecterDuBrokerMQTT();

    bool estSujetPertinent(const string& sujet);
};

// Initialisation du vecteur statique pour stocker les passerelles précédentes
vector<pair<string, int>> ClientMQTT::passerellesPrecedentes;

// Implémentation du constructeur pour initialiser le client MQTT
ClientMQTT::ClientMQTT(const string& idClient, const string& hoteMQTT, int portMQTT, GestionnaireBaseDeDonnees& gestionnaireBD, const string& nomUtilisateurMQTT, const string& motDePasseMQTT, char* argv[])
    : mosquittopp(idClient.c_str()), hoteMQTT_(hoteMQTT), portMQTT_(portMQTT), gestionnaireBD_(gestionnaireBD), processeurMessage_(gestionnaireBD), nomUtilisateurMQTT_(nomUtilisateurMQTT), motDePasseMQTT_(motDePasseMQTT), argv_(argv), estEnRafraichissementAbonnements_(false)
{
    connecterAuBrokerMQTT();
    demarrerBoucleMQTT();
    dernierTempsMessage_ = chrono::system_clock::now();
    cout << "Client MQTT connecté et prêt." << endl;
}

// Implémentation du destructeur pour nettoyer les ressources du client MQTT
ClientMQTT::~ClientMQTT()
{
    arreterBoucleMQTT();
    deconnecterDuBrokerMQTT();
}

// Implémentation de la méthode pour vérifier les changements dans les passerelles
void ClientMQTT::verifierChangementsPasserelles()
{
    vector<pair<string, int>> passerellesActuelles = gestionnaireBD_.obtenirNomsPasserellesEtProtocoles();

    for (const auto& passerelleActuelle : passerellesActuelles)
    {
        bool trouve = false;
        for (const auto& passerellePrecedente : passerellesPrecedentes)
        {
            if (passerelleActuelle.first == passerellePrecedente.first && passerelleActuelle.second == passerellePrecedente.second)
            {
                trouve = true;
                break;
            }
        }

        if (!trouve)
        {
            if (passerelleActuelle.second == 1)
            {
                string sujet = "energy/consumption/" + passerelleActuelle.first + "/message/data/71435500-6791-11ce-97c6-313131303230";
                subscribe(nullptr, sujet.c_str());
                cout << "Nouvel abonnement au sujet : " << sujet << endl;
            }
        }
    }

    passerellesPrecedentes = passerellesActuelles;
}

// Implémentation de la méthode appelée lors de la connexion au broker MQTT
void ClientMQTT::on_connect(int rc)
{
    if (rc == 0)
    {
        cout << "Connecté au broker MQTT." << endl;
        vector<pair<string, int>> protocolesPasserelles = gestionnaireBD_.obtenirNomsPasserellesEtProtocoles();
        for (const auto& protocolePasserelle : protocolesPasserelles)
        {
            const string& nomPasserelle = protocolePasserelle.first;
            int idProtocole = protocolePasserelle.second;

            if (idProtocole == 1)
            {
                string sujet = "energy/consumption/" + nomPasserelle + "/message/data/71435500-6791-11ce-97c6-313131303230";
                subscribe(nullptr, sujet.c_str());
                cout << "Abonné au sujet : " << sujet << endl;
            }
            else
            {
                cout << "Passerelle " << nomPasserelle << " ignorée en raison de ID_Protocole_FK = " << idProtocole << endl;
            }
        }
    }
    else
    {
        cout << "Erreur de connexion au broker MQTT : " << rc << endl;
    }
}

// Implémentation de la méthode appelée lors de la réception d'un message MQTT
void ClientMQTT::on_message(const struct mosquitto_message* message)
{
    string sujet = message->topic;
    string chargeUtile(static_cast<char*>(message->payload), message->payloadlen);

    if (estSujetPertinent(sujet) && !estEnRafraichissementAbonnements_)
    {
        cout << "Message reçu sur le sujet : " << sujet << endl;
        processeurMessage_.traiterMessageEntrant(sujet, chargeUtile);
        dernierTempsMessage_ = chrono::system_clock::now();
    }
}

// Implémentation de la méthode pour rafraîchir les abonnements aux passerelles
void ClientMQTT::rafraichirAbonnementsPasserelles()
{
    estEnRafraichissementAbonnements_ = true;
    vector<pair<string, int>> protocolesPasserelles = gestionnaireBD_.obtenirNomsPasserellesEtProtocoles();
    for (const auto& protocolePasserelle : protocolesPasserelles)
    {
        const string& nomPasserelle = protocolePasserelle.first;
        int idProtocole = protocolePasserelle.second;

        if (idProtocole == 1)
        {
            string sujet = "energy/consumption/" + nomPasserelle + "/message/data/71435500-6791-11ce-97c6-313131303230";
            subscribe(nullptr, sujet.c_str());
            cout << "Abonné au sujet : " << sujet << endl;
        }
        else
        {
            cout << "Passerelle " << nomPasserelle << " ignorée en raison de ID_Protocole_FK = " << idProtocole << endl;
        }
    }
    estEnRafraichissementAbonnements_ = false;
}


void ClientMQTT::connecterAuBrokerMQTT()
{
    username_pw_set(nomUtilisateurMQTT_.c_str(), motDePasseMQTT_.c_str());
    connect_async(hoteMQTT_.c_str(), portMQTT_, 60);
}


void ClientMQTT::demarrerBoucleMQTT()
{
    loop_start();
}


void ClientMQTT::arreterBoucleMQTT()
{
    loop_stop();
}


void ClientMQTT::deconnecterDuBrokerMQTT()
{
    disconnect();
}


bool ClientMQTT::estSujetPertinent(const string& sujet)
{
    return sujet.find("energy/consumption/") != string::npos;
}

// Implémentation de la méthode pour obtenir le dernier temps de réception de message
chrono::system_clock::time_point ClientMQTT::obtenirDernierTempsMessage() const
{
    return dernierTempsMessage_;
}

// Variable globale pour gérer l'état d'exécution du programme
volatile sig_atomic_t enExecution = 1;

// Gestionnaire de signal pour arrêter le programme
void gestionnaireSignal(int sig)
{
    enExecution = 0;
}

// Fonction principale pour exécuter le programme
void executerProgramme(int argc, char* argv[])
{
    const string hoteMQTT = "217.182.60.210";
    int portMQTT = 1883;
    const string idClientMQTT = "cpp_mqtt_client";
    const string nomUtilisateurMQTT = "adminsn";
    const string motDePasseMQTT = "admincielir";

    const string hoteBD = "217.182.60.210";
    const string utilisateurBD = "etudiant";
    const string motDePasseBD = "admincielir";
    const string nomBD = "Mesure_De";

    cout << "Démarrage du client MQTT..." << endl;

    GestionnaireBaseDeDonnees gestionnaireBD(hoteBD, utilisateurBD, motDePasseBD, nomBD);
    ClientMQTT clientMQTT(idClientMQTT, hoteMQTT, portMQTT, gestionnaireBD, nomUtilisateurMQTT, motDePasseMQTT, argv);

    while (enExecution)
    {
        this_thread::sleep_for(chrono::minutes(1));

        auto maintenant = chrono::system_clock::now();
        auto dernierTempsMessage = clientMQTT.obtenirDernierTempsMessage();
        auto tempsDepuisDernierMessage = chrono::duration_cast<chrono::minutes>(maintenant - dernierTempsMessage).count();

        if (tempsDepuisDernierMessage >= 11)
        {
            cout << "Aucun message reçu depuis 11 minutes. Rafraîchissement des abonnements..." << endl;
            cout << "=========================================================================" << endl;
            clientMQTT.rafraichirAbonnementsPasserelles();
        }

        clientMQTT.verifierChangementsPasserelles();
    }
}

// Fonction principale du programme
int main(int argc, char* argv[])
{
    signal(SIGINT, gestionnaireSignal);

    executerProgramme(argc, argv);

    return 0;
}
