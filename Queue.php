<?php
/**
 * Created by PhpStorm.
 * User: 俊杰
 * Date: 14-9-12
 * Time: 上午11:16
 */

namespace iit\queue;


use yii\base\Component;
use yii\base\InvalidParamException;

abstract class Queue extends Component
{
    const LEVEL_NORMAL = 'Normal';
    const LEVEL_LOW = 'Low';
    const LEVEL_HIGH = 'High';
    private $_tmpMessageId;

    /**
     * 发送正常优先级别消息到队列
     * @param $msg
     * @return bool
     */

    public function sendMessage($msg)
    {
        return $this->send($msg, 0, self::LEVEL_NORMAL);
    }

    /**
     * 发送低优先级别消息到队列
     * @param $msg
     * @return bool
     */

    public function sendLowMessage($msg)
    {
        return $this->send($msg, 0, self::LEVEL_LOW);
    }

    /**
     * 发送高优先级别消息到队列
     * @param $msg
     * @return bool
     */

    public function sendHighMessage($msg)
    {
        return $this->send($msg, 0, self::LEVEL_HIGH);
    }

    /**
     * 发送正常优先级别延时消息到队列
     * @param $msg
     * @param $delay
     * @return bool
     */

    public function sendDelayMessage($msg, $delay)
    {
        return $this->send($msg, $delay, self::LEVEL_NORMAL);
    }

    /**
     * 发送低优先级别延时消息到队列
     * @param $msg
     * @param $delay
     * @return bool
     */

    public function sendLowDelayMessage($msg, $delay)
    {
        return $this->send($msg, $delay, self::LEVEL_LOW);
    }

    /**
     * 发送高常优先级别延时消息到队列
     * @param $msg
     * @param $delay
     * @return bool
     */

    public function sendHighDelayMessage($msg, $delay)
    {
        return $this->send($msg, $delay, self::LEVEL_HIGH);
    }

    /**
     * 发送消息实现方法，子类必须实现此方法
     * @param $msg
     * @param $delay
     * @param $level
     * @return bool
     */
    abstract protected function send($msg, $delay, $level);

    /**
     * 从队列中取出消息，并自动保存消息ID方便后续操作
     * @return mixed
     */

    public function receiveMessage()
    {
        $result = $this->receive();
        $this->setTmpMessageId($result['msgId']);
        return $result;
    }

    /**
     * 从队列中取出消息实现方法，子类必须实现此方法
     * @return mixed
     */

    abstract protected function receive();

    /**
     * 删除被取出并成功消费的消息
     * @param null $msgId
     * @return mixed
     */

    public function deleteMessage($msgId = null)
    {
        if ($msgId === null) {
            $msgId = $this->getTmpMessageId();
        }
        return $this->delete($msgId);
    }

    /**
     * 删除消息实现方法，子类必须实现此类
     * @param $msgId
     * @return mixed
     */

    abstract protected function delete($msgId);

    /**
     * 设置被取出消息超时时间
     * @param null $msgId
     * @param $timeout
     * @return mixed
     */

    public function setTimeout($timeout, $msgId = null)
    {
        if ($msgId === null) {
            $msgId = $this->getTmpMessageId();
        }
        $result = $this->setVisibilityTimeout($msgId, $timeout);
        $this->setTmpMessageId($result['msgId']);
        return $result;
    }

    /**
     * 设置被取出消息超时时间实现方法，子类必须实现此方法
     * @param $msgId
     * @param $timeout
     * @return mixed
     */

    abstract protected function setVisibilityTimeout($msgId, $timeout);

    /**
     * 设置取出消息的ID
     * @param $msgId
     */

    public function setTmpMessageId($msgId)
    {
        $this->_tmpMessageId = $msgId;
    }

    /**
     * 获取取出消息的ID
     * @return mixed
     * @throws \yii\base\InvalidParamException
     */

    public function getTmpMessageId()
    {
        if ($this->_tmpMessageId === null) {
            throw new InvalidParamException('Not Found TmpMessageId,Please Use receiveMessage Function');
        }
        return $this->_tmpMessageId;
    }
} 