<?php

namespace App\Services;

use Elasticsearch\ClientBuilder;

// 单例模式封装es
class ElasticConnect {

    /**
     * ES的ip
     *
     * @var string
     */
    const ESHOSTNAME = '127.0.0.1';

    /**
     * ES的port
     *
     * @var int
     */
    const ESPORT = 9200;

    /**
     * 类单例
     *
     * @var object
     */
    private static $instance;
    /**
     * Es的连接句柄
     *
     * @var object
     */
    private $elastic;
    /**
     * 私有化构造函数，防止类外实例化
     *
     * @param
     */
    private function __construct ()
    {
        $this->elastic = ClientBuilder::create()->setHosts([self::ESHOSTNAME])->build();
    }
    /**
     * 私有化克隆函数，防止类外克隆对象
     */
    private function __clone ()
    {}
    /**
     * 类的唯一公开静态方法，获取类单例的唯一入口
     *
     * @return object
     */
    public static function getEslasticInstance ()
    {
        if (! (self::$instance instanceof self)) {
            self::$instance = new self();
        }
        return self::$instance;
    }
    /**
     * 获取Es的连接实例
     *
     * @return
     */
    public function getEsConn ()
    {
        return $this->elastic;
    }

    /**
     * 将数据放入es
     * @param $index
     * @param $data
     * @return array|callable
     */
    public function add($index,$data){
        foreach ($data as $val){
            $params=[
                'index'=>$index,
                'type'=>'_doc',
                'id'=>$val['id'],
                'body'=>$val
            ];
            $response = $this->elastic->index($params);
        }
        return $response;
    }

    /**
     * 将单个数据放入es
     * @param $index
     * @param $data
     * @return mixed
     */
    public function addOne($index,$data){
        $params=[
            'index'=>$index,
            'type'=>'_doc',
            'id'=>$data['goods_id'],
            'body'=>$data
        ];
        $response = $this->elastic->index($params);

        return $response;
    }

    /**
     * 添加索引 中文分词
     * @param $index
     * @param $type
     * @param $field   // 字段索引
     * @return array
     */
    public function creation_index($index,$type,$field){
        $params = [
            'index' => $index,
            'body' => [
                'mappings' => [
                    $type=>[
                        '_source'=>[
                            'enabled'=>true
                        ],
                        'properties'=>[
                            $field=>[
                                'type'=>'text',
                                'analyzer'=>'ik_smart',//ik分词器
                            ]
                        ]
                    ]
                ]
            ],
            'include_type_name' => true,
        ];
        $response = $this->elastic->indices()->create($params);
        return $response;
    }

    /**
     * 高亮搜索
     * @param $index // 索引类型
     * @param $name // 搜索内容
     * @param $search // 搜索字段
     * @return array
     */
    public function search($index,$name,$search){
        $params = [
            'index' => $index,
            'type' => '_doc',
            'body' => [
                'query' => [
                    'match' => [
                        $search => $name
                    ]
                ],
                'highlight'=>[
                    'fields'=>[
                        $search=>[
                            'pre_tags'=>["<span style='color: red'>"],
                            'post_tags'=>["</span>"]
                        ]
                    ]
                ]
            ]
        ];
        //处理数据
        $results=$this->elastic->search($params);
        $data=$results['hits']['hits'];
        foreach ($data as $k=>$v){
            $data[$k]['_source'][$search]=$v['highlight'][$search][0];

        }
        $title=array_column($data,'_source');
        return $title;
    }


    /**
     * 删除数据
     * @param $index
     * @param $id
     * @return array|callable
     */
    public function delData($index,$id){
        $params = [
            'index' => $index,
            'type' => '_doc',
            'id' => $id,
        ];
        $response = $this->elastic->delete($params);
        return $response;
    }

    /**
     * 需要在单例切换的时候做清理工作
     */
    public function __destruct ()
    {
        self::$instance->elastic->close();
        self::$instance = NULL;
    }
}
