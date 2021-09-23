<?php


namespace Fairy;


use Workerman\Connection\TcpConnection;
use Workerman\Crontab\Crontab;
use Workerman\MySQL\Connection;
use Workerman\Protocols\Http\Request;
use Workerman\Protocols\Http\Response;
use Workerman\Worker;

/**
 * 注意：定时器开始、暂停、重起 都是在下一分钟开始执行
 * Class CrontabService
 * @package Fairy
 */
class HttpCrontabService
{
    const FORBIDDEN_STATUS = '0';
    const NORMAL_STATUS = '1';

    //请求接口地址
    const INDEX_PATH = 'crontab/index';
    const ADD_PATH = 'crontab/add';
    const MODIFY_PATH = 'crontab/modify';
    const RELOAD_PATH = 'crontab/reload';
    const DELETE_PATH = 'crontab/delete';
    const FLOW_PATH = 'crontab/flow';
    const POOL_PATH = 'crontab/pool';
    const PING_PATH = 'crontab/ping';

    /**
     * 监听任何端口
     * @var string
     */
    private static $socketName = 'http://127.0.0.1:2345';

    /**
     * worker 实例
     * @var Worker
     */
    private $worker;

    /**
     * 进程名
     * @var string
     */
    private $workerName = "Workerman Visible Crontab";

    /**
     * 数据库配置
     * @var array
     */
    private $dbConfig = [
        'hostname' => '127.0.0.1',
        'hostport' => '3306',
        'username' => 'root',
        'password' => 'root',
        'database' => 'test',
        'charset' => 'utf8mb4'
    ];

    /**
     * 数据库进程池
     * @var Connection[] array
     */
    private $dbPool = [];

    /**
     * 任务进程池
     * @var Crontab[] array
     */
    private $crontabPool = [];

    /**
     * 调试模式
     * @var bool
     */
    private $debug = false;

    /**
     * 错误信息
     * @var
     */
    private $errorMsg = [];

    /**
     * 定时任务表
     * @var string
     */
    private $systemCrontabTable = 'system_crontab';

    /**
     * 定时任务日志表
     * @var string
     */
    private $systemCrontabFlowTable = 'system_crontab_flow';

    /**
     * 定时任务日志表后缀 按月分表
     * @var string|null
     */
    private $systemCrontabFlowTableSuffix;

    /**
     * 最低PHP版本
     * @var string
     */
    private $lessPhpVersion = '5.3.3';

    /**
     * @var Request
     */
    private $request;

    /**
     * @param string $socketName 不填写表示不监听任何端口,格式为 <协议>://<监听地址> 协议支持 tcp、udp、unix、http、websocket、text
     * @param array $contextOption socket 上下文选项 http://php.net/manual/zh/context.socket.php
     */
    public function __construct($socketName = '', array $contextOption = [])
    {
        $this->checkEnv();
        $this->initWorker($socketName, $contextOption);
    }

    /**
     * 初始化 worker
     * @param string $socketName
     * @param array $contextOption
     */
    private function initWorker($socketName = '', $contextOption = [])
    {
        $socketName && self::$socketName = $socketName;
        $this->worker = new Worker(self::$socketName, $contextOption);
        $this->worker->name = $this->workerName;
        if (isset($contextOption['ssl'])) {
            $this->worker->transport = 'ssl';//设置当前Worker实例所使用的传输层协议，目前只支持3种(tcp、udp、ssl)。默认为tcp。
        }
        $this->registerCallback();
    }

    /**
     * 是否调试模式
     * @param bool $bool
     * @return $this
     */
    public function setDebug($bool = false)
    {
        $this->debug = $bool;

        return $this;
    }

    /**
     * 设置当前Worker实例的名称,方便运行status命令时识别进程
     * 默认为none
     * @param string $name
     * @return $this
     */
    public function setName($name = "Workerman Visible Crontab")
    {
        $this->worker->name = $name;

        return $this;
    }

    /**
     * 设置当前Worker实例启动多少个进程
     * Worker主进程会 fork出 count个子进程同时监听相同的端口，并行的接收客户端连接，处理连接上的事件
     * 默认为1
     * windows系统不支持此特性
     * @param int $count
     * @return $this
     */
    public function setCount($count = 1)
    {
        $this->worker->count = $count;

        return $this;
    }

    /**
     * 设置当前Worker实例以哪个用户运行
     * 此属性只有当前用户为root时才能生效，建议$user设置权限较低的用户
     * 默认以当前用户运行
     * windows系统不支持此特性
     * @param string $user
     * @return $this
     */
    public function setUser($user = "root")
    {
        $this->worker->user = $user;

        return $this;
    }

    /**
     * 设置当前Worker实例的协议类
     * 协议处理类可以直接在实例化Worker时在监听参数直接指定
     * @param string $protocol
     * @return $this
     */
    public function setProtocol($protocol)
    {
        $this->worker->protocol = $protocol;

        return $this;
    }

    /**
     * 以daemon(守护进程)方式运行
     * windows系统不支持此特性
     * @param bool $bool
     * @return $this
     */
    public function setDaemon($bool = false)
    {
        Worker::$daemonize = $bool;

        return $this;
    }

    /**
     * 设置所有连接的默认应用层发送缓冲区大小。默认1M。可以动态设置
     * @param float|int $size
     * @return $this
     */
    public function setMaxSendBufferSize($size = 1024 * 1024)
    {
        TcpConnection::$defaultMaxSendBufferSize = $size;

        return $this;
    }

    /**
     * 设置每个连接接收的数据包。默认10M。超包视为非法数据，连接会断开
     * @param float|int $size
     * @return $this
     */
    public function setMaxPackageSize($size = 10 * 1024 * 1024)
    {
        TcpConnection::$defaultMaxPackageSize = $size;

        return $this;
    }

    /**
     * 指定日志文件
     * 默认为位于workerman下的 workerman.log
     * 日志文件中仅仅记录workerman自身相关启动停止等日志，不包含任何业务日志
     * @param string $path
     * @return $this
     */
    public function setLogFile($path = "./workerman.log")
    {
        Worker::$logFile = $path;

        return $this;
    }

    /**
     * 指定打印输出文件
     * 以守护进程方式(-d启动)运行时，所有向终端的输出(echo var_dump等)都会被重定向到 stdoutFile指定的文件中
     * 默认为/dev/null,也就是在守护模式时默认丢弃所有输出
     * windows系统不支持此特性
     * @param string $path
     * @return $this
     */
    public function setStdoutFile($path = "./workerman_debug.log")
    {
        Worker::$stdoutFile = $path;

        return $this;
    }

    /**
     * 设置数据库链接信息
     * @param array $config
     * @return $this
     */
    public function setDbConfig(array $config = [])
    {
        $this->dbConfig = array_merge($this->dbConfig, $config);

        return $this;
    }

    /**
     * 注册子进程回调函数
     */
    private function registerCallback()
    {
        $this->worker->onWorkerStart = [$this, 'onWorkerStart'];
        $this->worker->onWorkerReload = [$this, 'onWorkerReload'];
        $this->worker->onWorkerStop = [$this, 'onWorkerStop'];
        $this->worker->onConnect = [$this, 'onConnect'];
        $this->worker->onMessage = [$this, 'onMessage'];
        $this->worker->onClose = [$this, 'onClose'];
        $this->worker->onBufferFull = [$this, 'onBufferFull'];
        $this->worker->onBufferDrain = [$this, 'onBufferDrain'];
        $this->worker->onError = [$this, 'onError'];
    }

    /**
     * 设置Worker子进程启动时的回调函数，每个子进程启动时都会执行
     * @param Worker $worker
     */
    public function onWorkerStart($worker)
    {
        $this->dbPool[$worker->id] = new Connection(
            $this->dbConfig['hostname'],
            $this->dbConfig['hostport'],
            $this->dbConfig['username'],
            $this->dbConfig['password'],
            $this->dbConfig['database'],
            $this->dbConfig['charset']
        );
        $this->checkCrontabTables();
        $this->crontabInit();
    }

    /**
     * @param Worker $worker
     */
    public function onWorkerStop($worker)
    {

    }

    /**
     * 设置Worker收到reload信号后执行的回调
     * 如果在收到reload信号后只想让子进程执行onWorkerReload，不想退出，可以在初始化Worker实例时设置对应的Worker实例的reloadable属性为false
     * @param Worker $worker
     */
    public function onWorkerReload($worker)
    {

    }

    /**
     * 当客户端与Workerman建立连接时(TCP三次握手完成后)触发的回调函数
     * 每个连接只会触发一次onConnect回调
     * 此时客户端还没有发来任何数据
     * 由于udp是无连接的，所以当使用udp时不会触发onConnect回调，也不会触发onClose回调
     * @param TcpConnection $connection
     */
    public function onConnect($connection)
    {
        $this->checkCrontabTables();
    }

    /**
     * 当客户端连接与Workerman断开时触发的回调函数
     * 不管连接是如何断开的，只要断开就会触发onClose
     * 每个连接只会触发一次onClose
     * 由于udp是无连接的，所以当使用udp时不会触发onConnect回调，也不会触发onClose回调
     * @param TcpConnection $connection
     */
    public function onClose($connection)
    {

    }

    /**
     * 当客户端通过连接发来数据时(Workerman收到数据时)触发的回调函数
     * @param TcpConnection $connection
     * @param $data
     */
    public function onMessage($connection, $request)
    {
        if ($request instanceof Request) {
            $this->request = $request;
            switch (ltrim($request->path(), '/')) {
                case self::INDEX_PATH:
                    list($page, $limit, $where) = $this->buildTableParames();
                    $response = $this->crontabIndex($page, $limit, $where);
                    break;
                case self::ADD_PATH:
                    if ($post = $request->post()) {
                        $response = $this->crontabAdd($post);
                    }
                    break;
                case self::MODIFY_PATH:
                    if ($post = $request->post()) {
                        $response = $this->crontabModify($post['id'], $post['field'], $post['value']);
                    }
                    break;
                case self::DELETE_PATH:
                    $id = $request->post('id', '');
                    if (!empty($id)) {
                        $response = $this->crontabDelete($id);
                    }
                    break;
                case self::RELOAD_PATH:
                    $id = $request->post('id', '');
                    if (!empty($id)) {
                        $response = $this->crontabReload($id);
                    }
                    break;
                case self::FLOW_PATH:
                    list($page, $limit, $where, $excludeFields) = $this->buildTableParames(['month']);
                    $request->get('sid') && $where[] = ['sid', '=', $request->get('sid')];
                    $response = $this->crontabFlow($page, $limit, $where, $excludeFields);
                    break;
                case self::POOL_PATH:
                    $response = $this->crontabPool();
                    break;
                case self::PING_PATH:
                default:
                    $response = 'pong';
                    break;
            }
            $connection->send($this->response(isset($response) ? $response : false));
        }
    }

    /**
     * 缓冲区满则会触发onBufferFull回调
     * 每个连接都有一个单独的应用层发送缓冲区，如果客户端接收速度小于服务端发送速度，数据会在应用层缓冲区暂存
     * 只要发送缓冲区还没满，哪怕只有一个字节的空间，调用Connection::send($A)肯定会把$A放入发送缓冲区,
     * 但是如果已经没有空间了，还继续Connection::send($B)数据，则这次send的$B数据不会放入发送缓冲区，而是被丢弃掉，并触发onError回调
     * @param TcpConnection $connection
     */
    public function onBufferFull($connection)
    {

    }

    /**
     * 在应用层发送缓冲区数据全部发送完毕后触发
     * @param TcpConnection $connection
     */
    public function onBufferDrain($connection)
    {

    }

    /**
     * 客户端的连接上发生错误时触发
     * @param TcpConnection $connection
     * @param $code
     * @param $msg
     */
    public function onError($connection, $code, $msg)
    {

    }

    private function getCrontabById($id)
    {
        return $this->dbPool[$this->worker->id]
            ->select('*')
            ->from($this->systemCrontabTable)
            ->where('id= :id')
            ->bindValues(['id' => $id])
            ->row();
    }

    /**
     * 初始化定时任务
     * @return bool
     */
    private function crontabInit()
    {
        $ids = $this->dbPool[$this->worker->id]
            ->select('id')
            ->from($this->systemCrontabTable)
            ->where("status= :status")
            ->bindValues(['status' => self::NORMAL_STATUS])
            ->orderByDESC(['sort'])
            ->column();

        if (!empty($ids)) {
            foreach ($ids as $id) {
                $this->crontabRun($id);
            }
        }

        return true;
    }

    /**
     * 定时器列表
     * @param int $page
     * @param int $limit
     * @param array $where
     * @return array
     */
    private function crontabIndex($page, $limit, $where)
    {
        list($whereStr, $bindValues) = $this->parseWhere($where);

        $data = $this->dbPool[$this->worker->id]
            ->select('*')
            ->from($this->systemCrontabTable)
            ->where($whereStr)
            ->orderByDESC(['Id'])
            ->limit($limit)
            ->offset(($page - 1) * $limit)
            ->bindValues($bindValues)
            ->query();

        $count = $this->dbPool[$this->worker->id]
            ->select('count(id)')
            ->from($this->systemCrontabTable)
            ->where($whereStr)
            ->bindValues($bindValues)
            ->single();

        return ['list' => $data, 'count' => $count];
    }

    /**
     * 创建定时任务
     * @param array $data
     * @return bool
     */
    private function crontabAdd(array $data)
    {
        $data['create_time'] = $data['update_time'] = time();
        $id = $this->dbPool[$this->worker->id]
            ->insert($this->systemCrontabTable)
            ->cols($data)
            ->query();
        $id && $this->crontabRun($id);

        return $id ? true : false;
    }

    /**
     * 修改定时器
     * @param $id
     * @param $field
     * @param $value
     * @return mixed
     */
    private function crontabModify($id, $field, $value)
    {
        if (in_array($field, ['status', 'sort', 'remark', 'title', 'frequency'])) {
            $row = $this->dbPool[$this->worker->id]
                ->update($this->systemCrontabTable)
                ->cols([$field])
                ->where('id = :id')
                ->bindValues(['id' => $id, $field => $value])
                ->query();

            if ($field === 'status') {
                if ($value == self::NORMAL_STATUS) {
                    $this->crontabRun($id);
                } else {
                    $this->crontabDelete($id, false);
                }
            }

            return $row ? true : false;
        } else {
            return false;
        }
    }

    /**
     * 清除定时任务
     * @param $id
     * @param bool $db 是否删除数据
     * @return bool|mixed
     */
    private function crontabDelete($id, $db = true)
    {
        $ids = explode(',', $id);

        foreach ($ids as $item) {
            if (isset($this->crontabPool[$item])) {
                $this->crontabPool[$item]['crontab']->destroy();
                unset($this->crontabPool[$item]);
            }
        }

        if ($db) {
            $rows = $this->dbPool[$this->worker->id]
                ->delete($this->systemCrontabTable)
                ->where('id in (' . $id . ')')
                ->query();

            return $rows ? true : false;
        }

        return true;
    }

    /**
     * 重启定时任务
     * @param $id
     * @return bool
     */
    private function crontabReload($id)
    {
        $ids = explode(',', $id);

        foreach ($ids as $id) {
            $this->crontabDelete($id, false);
            $this->dbPool[$this->worker->id]
                ->update($this->systemCrontabTable)
                ->cols(['status'])
                ->where('id= :id')
                ->bindValues(['id' => $id, 'status' => self::NORMAL_STATUS])
                ->query();
            $this->crontabRun($id);
        }

        return true;
    }

    /**
     * 创建定时器
     * 0   1   2   3   4   5
     * |   |   |   |   |   |
     * |   |   |   |   |   +------ day of week (0 - 6) (Sunday=0)
     * |   |   |   |   +------ month (1 - 12)
     * |   |   |   +-------- day of month (1 - 31)
     * |   |   +---------- hour (0 - 23)
     * |   +------------ min (0 - 59)
     * +-------------- sec (0-59)[可省略，如果没有0位,则最小时间粒度是分钟]
     * @param int $id
     */
    private function crontabRun($id)
    {
        $data = $this->dbPool[$this->worker->id]
            ->select('*')
            ->from($this->systemCrontabTable)
            ->where('id= :id')
            ->where('status= :status')
            ->bindValues(['id' => $id, 'status' => self::NORMAL_STATUS])
            ->row();

        if (!empty($data)) {
            $this->crontabPool[$data['id']] = [
                'id' => $data['id'],
                'shell' => $data['shell'],
                'frequency' => $data['frequency'],
                'remark' => $data['remark'],
                'create_time' => date('Y-m-d H:i:s'),
                'crontab' => new Crontab($data['frequency'], function () use ($data) {
                    $time = time();
                    $shell = trim($data['shell']);
                    $this->debug && $this->writeln('执行定时器任务#' . $data['id'] . ' ' . $data['frequency'] . ' ' . $shell);
                    $startTime = microtime(true);
                    exec($shell, $output, $code);
                    $endTime = microtime(true);
                    $this->dbPool[$this->worker->id]->query("UPDATE {$this->systemCrontabTable} SET running_times = running_times + 1, last_running_time = {$time} WHERE id = {$data['id']}");
                    $this->crontabRunLog([
                        'sid' => $data['id'],
                        'command' => $shell,
                        'output' => join(PHP_EOL, $output),
                        'return_var' => $code,
                        'running_time' => round($endTime - $startTime, 6),
                        'create_time' => $time,
                        'update_time' => $time,
                    ]);

                })
            ];
        }
    }

    /**
     * 定时器池
     * @return array
     */
    private function crontabPool()
    {
        $data = [];
        foreach ($this->crontabPool as $row) {
            unset($row['crontab']);
            $data[] = $row;
        }

        return $data;
    }

    /**
     * 执行日志
     * @param $page
     * @param $limit
     * @param $where
     * @param $excludeFields
     * @return array
     */
    private function crontabFlow($page, $limit, $where, $excludeFields)
    {
        list($whereStr, $bindValues) = $this->parseWhere($where);

        $allTables = $this->getDbTables($this->dbConfig['database']);
        $tableName = isset($excludeFields['month']) && !empty($excludeFields['month']) ?
            preg_replace('/_\d+/', '_' . date('Ym', strtotime($excludeFields['month'])), $this->systemCrontabFlowTable) :
            $this->systemCrontabFlowTable;
        $data = [];
        $count = 0;
        if (in_array($tableName, $allTables)) {
            $data = $this->dbPool[$this->worker->id]
                ->select('*')
                ->from($tableName)
                ->where($whereStr)
                ->orderByDESC(['Id'])
                ->limit($limit)
                ->offset(($page - 1) * $limit)
                ->bindValues($bindValues)
                ->query();

            $count = $this->dbPool[$this->worker->id]
                ->select('count(id)')
                ->from($tableName)
                ->where($whereStr)
                ->bindValues($bindValues)
                ->single();
        }

        return ['list' => $data, 'count' => $count];
    }

    /**
     * 记录执行日志
     * @param array $data
     * @return mixed
     */
    private function crontabRunLog(array $data)
    {
        return $this->dbPool[$this->worker->id]
            ->insert($this->systemCrontabFlowTable)
            ->cols($data)
            ->query();
    }

    /**
     * 获取socketName
     * @return string
     */
    public static function getSocketName()
    {
        return self::$socketName;
    }

    public static function getCrontabIndexPath()
    {
        return self::getSocketName() . '/' . self::INDEX_PATH;
    }

    public static function getCrontabAddPath()
    {
        return self::getSocketName() . '/' . self::ADD_PATH;
    }

    public static function getCrontabModifyPath()
    {
        return self::getSocketName() . '/' . self::MODIFY_PATH;
    }

    public static function getCrontabReloadPath()
    {
        return self::getSocketName() . '/' . self::RELOAD_PATH;
    }

    public static function getCrontabDeletePath()
    {
        return self::getSocketName() . '/' . self::DELETE_PATH;
    }

    public static function getCrontabFlowPath()
    {
        return self::getSocketName() . '/' . self::FLOW_PATH;
    }

    public static function getCrontabPoolPath()
    {
        return self::getSocketName() . '/' . self::POOL_PATH;
    }

    public static function getCrontabPingPath()
    {
        return self::getSocketName() . '/' . self::PING_PATH;
    }

    /**
     * 函数是否被禁用
     * @param $method
     * @return bool
     */
    private function functionDisabled($method)
    {
        return in_array($method, explode(',', ini_get('disable_functions')));
    }

    /**
     * 扩展是否加载
     * @param $extension
     * @return bool
     */
    private function extensionLoaded($extension)
    {
        return in_array($extension, get_loaded_extensions());
    }

    /**
     * 是否是Linux操作系统
     * @return bool
     */
    private function isLinux()
    {
        return strpos(PHP_OS, "Linux") !== false ? true : false;
    }

    /**
     * 版本比较
     * @param $version
     * @param string $operator
     * @return bool
     */
    private function versionCompare($version, $operator = ">=")
    {
        return version_compare(phpversion(), $version, $operator);
    }

    /**
     * 检测运行环境
     */
    private function checkEnv()
    {
        $errorMsg = [];
        $this->functionDisabled('exec') && $errorMsg[] = 'exec函数被禁用';
        if ($this->isLinux()) {
            $this->versionCompare($this->lessPhpVersion, '<') && $errorMsg[] = 'PHP版本必须≥' . $this->lessPhpVersion;
            $checkExt = ["pcntl", "posix"];
            foreach ($checkExt as $ext) {
                !$this->extensionLoaded($ext) && $errorMsg[] = $ext . '扩展没有安装';
            }
            $checkFunc = [
                "stream_socket_server",
                "stream_socket_client",
                "pcntl_signal_dispatch",
                "pcntl_signal",
                "pcntl_alarm",
                "pcntl_fork",
                "posix_getuid",
                "posix_getpwuid",
                "posix_kill",
                "posix_setsid",
                "posix_getpid",
                "posix_getpwnam",
                "posix_getgrnam",
                "posix_getgid",
                "posix_setgid",
                "posix_initgroups",
                "posix_setuid",
                "posix_isatty",
            ];
            foreach ($checkFunc as $func) {
                $this->functionDisabled($func) && $errorMsg[] = $func . '函数被禁用';
            }
        }

        if (!empty($errorMsg)) {
            $this->errorMsg = array_merge($this->errorMsg, $errorMsg);
        }
    }

    /**
     * 输出日志
     * @param $msg
     * @param bool $ok
     */
    private function writeln($msg, $ok = true)
    {
        echo '[' . date('Y-m-d H:i:s') . '] ' . $msg . ($ok ? " [Ok] " : " [Fail] ") . PHP_EOL;
    }

    /**
     * 检测表是否存在
     */
    private function checkCrontabTables()
    {
        $date = date('Ym', time());
        if ($date !== $this->systemCrontabFlowTableSuffix) {
            $this->systemCrontabFlowTableSuffix = $date;
            $this->systemCrontabFlowTable .= "_" . $date;
            $allTables = $this->getDbTables($this->dbConfig['database']);
            !in_array($this->systemCrontabTable, $allTables) && $this->createSystemCrontabTable();
            !in_array($this->systemCrontabFlowTable, $allTables) && $this->createSystemCrontabFlowTable();
        }
    }

    /**
     * 创建定时器任务表
     */
    private function createSystemCrontabTable()
    {
        $sql = <<<SQL
 CREATE TABLE IF NOT EXISTS `{$this->systemCrontabTable}`  (
  `id` int(11) UNSIGNED NOT NULL AUTO_INCREMENT,
  `title` varchar(60) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '任务标题',
  `type` tinyint(4) NOT NULL DEFAULT 0 COMMENT '任务类型[0请求url,1执行sql,2执行shell]',
  `frequency` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '任务频率',
  `shell` varchar(150) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '任务脚本',
  `running_times` int(11) NOT NULL DEFAULT '0' COMMENT '已运行次数',
  `last_running_time` int(11) NOT NULL DEFAULT '0' COMMENT '最近运行时间',
  `remark` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '任务备注',
  `sort` int(11) NOT NULL DEFAULT 0 COMMENT '排序，越大越前',
  `status` tinyint(4) NOT NULL DEFAULT 0 COMMENT '任务状态状态[0:禁用;1启用]',
  `create_time` int(11) NOT NULL DEFAULT 0 COMMENT '创建时间',
  `update_time` int(11) NOT NULL DEFAULT 0 COMMENT '更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `title`(`title`) USING BTREE,
  INDEX `type`(`type`) USING BTREE,
  INDEX `create_time`(`create_time`) USING BTREE,
  INDEX `status`(`status`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci COMMENT = '定时器任务表' ROW_FORMAT = DYNAMIC
SQL;

        return $this->dbPool[$this->worker->id]->query($sql);
    }

    /**
     * 定时器任务流水表
     */
    private function createSystemCrontabFlowTable()
    {
        $sql = <<<SQL
CREATE TABLE IF NOT EXISTS `{$this->systemCrontabFlowTable}`  (
  `id` int(11) UNSIGNED NOT NULL AUTO_INCREMENT,
  `sid` int(60) NOT NULL COMMENT '任务id',
  `command` varchar(255) NOT NULL COMMENT '执行命令',
  `output` text NOT NULL COMMENT '执行输出',
  `return_var` tinyint(4) NOT NULL COMMENT '执行返回状态[0成功; 1失败]',
  `running_time` varchar(10) NOT NULL COMMENT '执行所用时间',
  `create_time` int(11) NOT NULL DEFAULT 0 COMMENT '创建时间',
  `update_time` int(11) NOT NULL DEFAULT 0 COMMENT '更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `create_time`(`create_time`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci COMMENT = '定时器任务流水表{$this->systemCrontabFlowTableSuffix}' ROW_FORMAT = DYNAMIC
SQL;

        return $this->dbPool[$this->worker->id]->query($sql);
    }

    /**
     * 获取数据库表名
     * @param $dbname
     * @return array
     */
    private function getDbTables($dbname)
    {
        return $this->dbPool[$this->worker->id]
            ->select('TABLE_NAME')
            ->from('information_schema.TABLES')
            ->where("TABLE_TYPE='BASE TABLE'")
            ->where("TABLE_SCHEMA='" . $dbname . "'")
            ->column();
    }

    private function response($data = '', $msg = '信息调用成功！', $code = 200)
    {
        return new Response(200, [
            'Content-Type' => 'application/json; charset=utf-8',
        ], json_encode(['code' => $code, 'data' => $data, 'msg' => $msg]));
    }

    /**
     * 构建请求参数
     * @param array $excludeFields 忽略构建搜索的字段
     * @return array
     */
    private function buildTableParames($excludeFields = [])
    {
        $get = $this->request->get();
        $page = isset($get['page']) && !empty($get['page']) ? (int)$get['page'] : 1;
        $limit = isset($get['limit']) && !empty($get['limit']) ? (int)$get['limit'] : 15;
        $filters = isset($get['filter']) && !empty($get['filter']) ? $get['filter'] : '{}';
        $ops = isset($get['op']) && !empty($get['op']) ? $get['op'] : '{}';
        // json转数组
        $filters = json_decode($filters, true);
        $ops = json_decode($ops, true);
        $where = [];
        $excludes = [];

        foreach ($filters as $key => $val) {
            if (in_array($key, $excludeFields)) {
                $excludes[$key] = $val;
                continue;
            }
            $op = isset($ops[$key]) && !empty($ops[$key]) ? $ops[$key] : '%*%';

            switch (strtolower($op)) {
                case '=':
                    $where[] = [$key, '=', $val];
                    break;
                case '%*%':
                    $where[] = [$key, 'LIKE', "%{$val}%"];
                    break;
                case '*%':
                    $where[] = [$key, 'LIKE', "{$val}%"];
                    break;
                case '%*':
                    $where[] = [$key, 'LIKE', "%{$val}"];
                    break;
                case 'range':
                    [$beginTime, $endTime] = explode(' - ', $val);
                    $where[] = [$key, '>=', strtotime($beginTime)];
                    $where[] = [$key, '<=', strtotime($endTime)];
                    break;
                default:
                    $where[] = [$key, $op, "%{$val}"];
            }
        }

        return [$page, $limit, $where, $excludes];
    }

    /**
     * 解析列表where条件
     * @param $where
     * @return array
     */
    private function parseWhere($where)
    {
        if (!empty($where)) {
            $whereStr = '';
            $bindValues = [];
            $whereCount = count($where);
            foreach ($where as $index => $item) {
                if ($item[0] === 'create_time') {
                    $whereStr .= $item[0] . ' ' . $item[1] . ' :' . $item[0] . $index . (($index == $whereCount - 1) ? ' ' : ' AND ');
                    $bindValues[$item[0] . $index] = $item[2];
                } else {
                    $whereStr .= $item[0] . ' ' . $item[1] . ' :' . $item[0] . (($index == $whereCount - 1) ? ' ' : ' AND ');
                    $bindValues[$item[0]] = $item[2];
                }
            }
        } else {
            $whereStr = '1 = 1';
            $bindValues = [];
        }

        return [$whereStr, $bindValues];
    }

    /**
     * 运行所有Worker实例
     * Worker::runAll()执行后将永久阻塞
     * windows版本的workerman不支持在同一个文件中实例化多个Worker
     * windows版本的workerman需要将多个Worker实例初始化放在不同的文件中
     */
    public function run()
    {
        if (empty($this->errorMsg)) {
            $this->writeln("启动系统任务");
            Worker::runAll();
        } else {
            foreach ($this->errorMsg as $v) {
                $this->writeln($v, false);
            }
        }
    }
}
