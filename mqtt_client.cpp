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

using json = nlohmann::json;
using namespace std;

class CDatabase 
{
public:
    CDatabase(const string& dbHost, const string& dbUser, const string& dbPassword, const string& dbName)
        : dbHost_(dbHost), dbUser_(dbUser), dbPassword_(dbPassword), dbName_(dbName),
          mysql_(nullptr, mysql_close) 
    {
        connectToDatabase();
    }

    ~CDatabase() 
    {
        disconnectFromDatabase();
    }

    vector<pair<string, int>> getGatewayNamesAndProtocols();
    int getDeviceId(const string& gatewayName);
    void insertMessageData(const string& gatewayName, time_t utcTimestamp, double differenceKWh, int deviceId);

private:
    string dbHost_, dbUser_, dbPassword_, dbName_;
    unique_ptr<MYSQL, decltype(&mysql_close)> mysql_;

    void connectToDatabase();
    void disconnectFromDatabase();
};

vector<pair<string, int>> CDatabase::getGatewayNamesAndProtocols() 
{
    vector<pair<string, int>> gatewayProtocols;
    string query = "SELECT Nom_dispositif, ID_Protocole_FK FROM Dispositif_Passerelle";
    mysql_query(mysql_.get(), query.c_str());

    MYSQL_RES* result = mysql_store_result(mysql_.get());
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result))) {
        gatewayProtocols.emplace_back(row[0], stoi(row[1]));
    }
    mysql_free_result(result);
    return gatewayProtocols;
}

int CDatabase::getDeviceId(const string& gatewayName) 
{
    string query = "SELECT ID_Dispositif_PK FROM Dispositif_Passerelle WHERE Nom_dispositif = '" + gatewayName + "' LIMIT 1";
    mysql_query(mysql_.get(), query.c_str());
    MYSQL_RES* result = mysql_store_result(mysql_.get());
    MYSQL_ROW row = mysql_fetch_row(result);
    if (!row) 
    {
        mysql_free_result(result);
        return -1;
    }
    int deviceId = stoi(row[0]);
    mysql_free_result(result);

    return deviceId;
}

void CDatabase::insertMessageData(const string& gatewayName, time_t utcTimestamp, double differenceKWh, int deviceId) 
{
    stringstream queryStream;
    queryStream << "INSERT INTO Donnee_Mesurer (Timestamp, Valeur_Mesure, ID_Dispositif_FK) VALUES (FROM_UNIXTIME("
                << utcTimestamp << "), " << differenceKWh << ", " << deviceId << ")";
    mysql_query(mysql_.get(), queryStream.str().c_str());
}

void CDatabase::connectToDatabase() 
{
    mysql_.reset(mysql_init(nullptr));
    if (mysql_real_connect(mysql_.get(), dbHost_.c_str(), dbUser_.c_str(), dbPassword_.c_str(), dbName_.c_str(), 0, nullptr, 0)) {
        cout << "Connecte a la base de donnees." << endl;
    } else {
        cout << "Erreur de connexion a la base de donnees." << endl;
    }
}

void CDatabase::disconnectFromDatabase() 
{
    mysql_.reset();
}

class MessageProcessor 
{
public:
    MessageProcessor(CDatabase& dbManager);

    void processIncomingMessage(const string& topic, const string& payload);
    void displayMessageInTerminal(const string& payload);
    void insertMessageIntoBDD(const string& topic, const string& payload);

private:
    CDatabase& dbManager_;
    double lastLoadValuesKwh_[6];
    int loadValueCount_;
    int messageCount_;

    string extractGatewayNameFromTopic(const string& topic);
};

MessageProcessor::MessageProcessor(CDatabase& dbManager)
    : dbManager_(dbManager), loadValueCount_(0), messageCount_(0) {}

void MessageProcessor::processIncomingMessage(const string& topic, const string& payload) 
{
    displayMessageInTerminal(payload);
    insertMessageIntoBDD(topic, payload);
}

void MessageProcessor::displayMessageInTerminal(const string& payload) 
{
    if (json::accept(payload)) 
    {
        auto jsonData = json::parse(payload);
        if (jsonData.contains("utctimestamp")) 
        {
            time_t timeVal = jsonData["utctimestamp"];
            cout << "UTC Timestamp: " << put_time(gmtime(&timeVal), "%Y-%m-%d %H:%M:%S") << endl;
        }
        if (jsonData["measures"].contains("Load_0_30001"))
        {
            double loadValueKWh = jsonData["measures"]["Load_0_30001"].get<double>() / 1000.0;
            cout << "Load_0_30001: " << loadValueKWh << " kWh" << endl;
        }
    }
}

void MessageProcessor::insertMessageIntoBDD(const string& topic, const string& payload) 
{
    auto jsonData = json::parse(payload);

    time_t utcTimestamp = jsonData["utctimestamp"];
    double currentLoadValueKWh = jsonData["measures"]["Load_0_30001"].get<double>() / 1000.0;

    string gatewayName = extractGatewayNameFromTopic(topic);
    int deviceId = dbManager_.getDeviceId(gatewayName);

    if (deviceId == -1) return;

    // Si on a moins de 6 valeurs de charge, on ajoute la nouvelle valeur
    if (loadValueCount_ < 6) 
    {
        lastLoadValuesKwh_[loadValueCount_++] = currentLoadValueKWh;
    } 
    else 
    {   
        // Si on a déjà 6 valeurs, on déplace les anciennes valeurs vers la gauche et on insère la nouvelle valeur à la fin
        for (int i = 0; i < 5; ++i) 
        {
            lastLoadValuesKwh_[i] = lastLoadValuesKwh_[i + 1];
        }
        lastLoadValuesKwh_[5] = currentLoadValueKWh;
    }

    cout << "taille : " << loadValueCount_ << endl;

    if (loadValueCount_ == 6) 
    {                                                 
        double diff = lastLoadValuesKwh_[5] - lastLoadValuesKwh_[0];
        messageCount_++;

        if (messageCount_ >= 1) 
        {
            dbManager_.insertMessageData(gatewayName, utcTimestamp, diff, deviceId);
            cout << "Données insérées dans la base de données." << endl;
            cout << "==============================" << endl;

            lastLoadValuesKwh_[0] = lastLoadValuesKwh_[5];
            loadValueCount_ = 1;
            messageCount_ = 0;
        }
    } 
    else 
    {
        cout << "Pas assez de valeurs pour " << gatewayName << ", insertion ignorée." << endl;
        cout << "===================================================" << endl;
    }
}

string MessageProcessor::extractGatewayNameFromTopic(const string& topic) 
{
    size_t start = topic.find("/", 18) + 1;
    size_t end = topic.find("/", start);
    return topic.substr(start, end - start);
}

class MqttClient : public mosqpp::mosquittopp
{
public:
    MqttClient(const string& clientId, const string& mqttHost, int mqttPort, CDatabase& dbManager);
    ~MqttClient();

    void on_connect(int rc) override;
    void on_message(const struct mosquitto_message* message) override;

    chrono::system_clock::time_point getLastMessageTime() const;

private:
    string mqttHost_;
    int mqttPort_;
    CDatabase& dbManager_;
    MessageProcessor messageProcessor_;
    chrono::system_clock::time_point lastMessageTime_;
    int messageCount_; 

    void connectToMqttBroker();
    void startMqttLoop();
    void stopMqttLoop();
    void disconnectFromMqttBroker();

    bool isRelevantTopic(const string& topic);
};

MqttClient::MqttClient(const string& clientId, const string& mqttHost, int mqttPort, CDatabase& dbManager)
    : mosquittopp(clientId.c_str()), mqttHost_(mqttHost), mqttPort_(mqttPort), dbManager_(dbManager), messageProcessor_(dbManager), messageCount_(0)
{
    connectToMqttBroker();
    startMqttLoop();
    lastMessageTime_ = chrono::system_clock::now();
    cout << "Client MQTT connecte et pret." << endl;
}

MqttClient::~MqttClient()
{
    stopMqttLoop();
    disconnectFromMqttBroker();
}

void MqttClient::on_connect(int rc)
{
    if (rc == 0)
    {
        cout << "Connecte au broker MQTT." << endl;
        vector<pair<string, int>> gatewayProtocols = dbManager_.getGatewayNamesAndProtocols();
        for (const auto& gatewayProtocol : gatewayProtocols)
        {
            const string& gatewayName = gatewayProtocol.first;
            int protocolId = gatewayProtocol.second;

            if (protocolId == 1)
            {
                string topic = "energy/consumption/" + gatewayName + "/message/data/71435500-6791-11ce-97c6-313131303230";
                subscribe(nullptr, topic.c_str());
                cout << "Abonne au sujet : " << topic << endl;
            }
            else
            {
                cout << "Passerelle " << gatewayName << " ignoree en raison de ID_Protocole_FK = " << protocolId << endl;
            }
        }
    }
    else
    {
        cout << "Erreur de connexion au broker MQTT : " << rc << endl;
    }
}

void MqttClient::on_message(const struct mosquitto_message* message)
{
    string topic = message->topic;
    string payload(static_cast<char*>(message->payload), message->payloadlen);

    if (isRelevantTopic(topic))
    {
        cout << "Message recu sur le sujet : " << topic << endl;
        messageProcessor_.processIncomingMessage(topic, payload);
        lastMessageTime_ = chrono::system_clock::now();
    }
}

void MqttClient::connectToMqttBroker()
{
    connect_async(mqttHost_.c_str(), mqttPort_, 60);
}

void MqttClient::startMqttLoop()
{
    loop_start();
}

void MqttClient::stopMqttLoop()
{
    loop_stop();
}

void MqttClient::disconnectFromMqttBroker()
{
    disconnect();
}

bool MqttClient::isRelevantTopic(const string& topic)
{
    return topic.find("energy/consumption/") != string::npos;
}

chrono::system_clock::time_point MqttClient::getLastMessageTime() const
{
    return lastMessageTime_;
}

volatile sig_atomic_t running = 1;

void signalHandler(int sig)
{
    running = 0;
}

int main(int argc, char* argv[])
{
    signal(SIGINT, signalHandler);

    const string mqttHost = "217.182.60.210";
    int mqttPort = 1883;
    const string mqttClientId = "cpp_mqtt_client";

    const string dbHost = "217.182.60.210";
    const string dbUser = "admin";
    const string dbPassword = "admin";
    const string dbName = "Mesure_De";

    cout << "Demarrage du client MQTT..." << endl;

    CDatabase dbManager(dbHost, dbUser, dbPassword, dbName);
    MqttClient mqttClient(mqttClientId, mqttHost, mqttPort, dbManager);

    while (running)
    {
        this_thread::sleep_for(chrono::seconds(60));

        auto now = chrono::system_clock::now();
        auto lastMessageTime = mqttClient.getLastMessageTime();
        auto duration = chrono::duration_cast<chrono::seconds>(now - lastMessageTime);

        if (duration.count() > 660)
        {
            cout << "Aucun message recu depuis 11 minutes, redemarrage complet du programme..." << endl;
            execvp(argv[0], argv);
            cerr << "Erreur lors du redemarrage : " << strerror(errno) << endl;
            break;
        }
    }
    return 0;
}
