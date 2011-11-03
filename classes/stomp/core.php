<?php

require_once Kohana::find_file('vendor', 'Stomp');

class Stomp_Core {
	static $instances = array();

	protected $_config_group;
	protected $_config;
	protected $_stomp;

	/**
	 *
	 * @param string $config_group
	 * @return Stomp
	 */
	public static function instance($config_group = 'default')
	{
		if ( ! isset(Stomp::$instances[$config_group]))
		{
			$instance = new Stomp($config_group);

			Stomp::$instances[$config_group] = $instance;
		}

		return Stomp::$instances[$config_group];
	}

	public function __construct($config_group)
	{
		$this->_config_group = $config_group;
		$this->_config = Kohana::$config->load('stomp.'.$config_group);

		$this->_stomp = new FuseForge_Stomp($this->_config['broker_uri']);
		$this->_stomp->sync = $this->_config['sync'];

		$this->_stomp->connect($this->_config['username'], $this->_config['password']);
		$this->_stomp->setReadTimeout($this->_config['read_timeout']);
	}

	public function __destruct()
	{
		return $this->_stomp->disconnect();
	}

	public function send($destination, $message, $properties = array(), $sync = NULL)
	{
		Kohana::$log->add(Log::DEBUG, "Stomp: Sending message to :destination", array(
			':destination' => $destination,
		));
		
		return $this->_stomp->send($destination, $message, $properties, $sync);
	}

	public function subscribe($destination, $properties = array(), $sync = NULL)
	{
		return $this->_stomp->subscribe($destination, $properties, $sync);
	}

	public function unsubscribe($destination, $properties = array(), $sync = NULL)
	{
		return $this->_stomp->unsubscribe($destination, $properties, $sync);
	}

	public function read()
	{
		return $this->_stomp->readFrame();
	}
	
	public function acknowledge($message)
    {
        return $this->_stomp->ack($message);
    }
}
